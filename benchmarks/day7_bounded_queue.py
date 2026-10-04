"""Observe bounded per-Consumer TX backlog and disconnect-on-overflow isolation.

Run Linux Debug SLOW/FAST 1800 Frames and FAST/FAST 300 Frames; distinguish
policy-induced SLOW EOF from crashes and protocol validation failures.
"""

import argparse
import asyncio
import csv
import hashlib
import json
import pathlib
import platform
import re
import time

import day5_compare as common
import day6_queue_growth as sampling

BUILD = pathlib.Path('out/day7-debug')
LIMIT = 32 * 1024 * 1024
EVENT_FIELDS = ('elapsed_ms', 'consumer_connection', 'pending_frames',
                'pending_bytes', 'incoming_frame_bytes', 'limit_bytes', 'policy')


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event-tree', type=pathlib.Path, default=pathlib.Path('.'))
    parser.add_argument('--out', type=pathlib.Path, default=pathlib.Path('/tmp/eventstreamlab-day7'))
    return parser.parse_args()


def parse_events(lines):
    events = []
    for line in lines:
        if line.startswith('queue_overflow '):
            values = dict(part.split('=', 1) for part in line.split()[1:])
            if set(values) != set(EVENT_FIELDS):
                raise ValueError(f'Malformed overflow event: {line}')
            events.append({k: values[k] if k == 'policy' else int(values[k]) for k in EVENT_FIELDS})
    return events


def diagnostic_lines(lines):
    return [line for line in lines if re.search(
        r'failed|mismatch|rejected|incomplete|unexpected|error', line, re.I)]


def expected_receive_termination(line):
    # Only transport termination may be expected after a corroborated overflow.
    # Validation, sequence and payload-pattern failures are never excused.
    return re.fullmatch(
        r'Failed to receive frame (header|payload): '
        r'(Peer closed connection (after receiving \d+ of \d+ requested bytes|before all requested bytes were received)'
        r'|recv\(\) failed \(error 104\))', line) is not None


def validate(record, processes, frames, expected_bytes, primary):
    server = record['server']
    if record['producer'].get('frames_sent') != frames or server['summary'].get('frames_received') != frames:
        raise ValueError('Producer/Server frame count mismatch')
    for role in ('server', 'producer', 'consumer2'):
        if processes[role].child.returncode != 0 or diagnostic_lines(processes[role].lines):
            raise ValueError(f'{role} process/correctness failure')
    for i in ('1', '2'):
        metrics = server['connections'][i]
        if metrics['max_pending_bytes'] > LIMIT:
            raise ValueError(f'Consumer {i} exact peak exceeds bound')
    for s in record['samples']:
        if not 0 <= s['pending_bytes'] <= LIMIT:
            raise ValueError('Queue sample exceeds bound')
        if s['pending_frames'] != s['frames_enqueued'] - s['frames_completed']:
            raise ValueError('Queue sample state mismatch')
    for event in record['events']:
        if (event['limit_bytes'] != LIMIT or event['policy'] != 'disconnect'
                or event['pending_bytes'] > LIMIT
                or event['pending_bytes'] + event['incoming_frame_bytes'] <= LIMIT):
            raise ValueError('Overflow event does not satisfy policy condition')
    normal_connections = ('2',) if primary else ('1', '2')
    for i in normal_connections:
        consumer = record['consumers'][i]
        metrics = server['connections'][i]
        if (processes[f'consumer{i}'].child.returncode != 0
                or diagnostic_lines(processes[f'consumer{i}'].lines)
                or consumer.get('frames_received') != frames or consumer.get('mode') != 'fast'
                or consumer.get('id') != (10 if i == '1' else 1)
                or any(metrics[k] != frames for k in ('frames_enqueued', 'frames_completed', 'frames_forwarded'))
                or metrics['bytes_sent'] != expected_bytes
                or metrics['queue_overflow_count'] != 0 or metrics['disconnected_due_to_overflow'] != 0):
            raise ValueError(f'Normal Consumer {i} correctness mismatch')
        group = [s for s in record['samples'] if s['consumer_connection'] == int(i)]
        if not group or group[0]['pending_bytes'] != 0 or group[-1]['pending_bytes'] != 0:
            raise ValueError(f'Normal Consumer {i} sampling lifecycle incomplete')
    if primary:
        events = record['events']
        slow = processes['consumer1']
        metrics = server['connections']['1']
        if (not events or any(e['consumer_connection'] != 1 for e in events)
                or metrics['disconnected_due_to_overflow'] != 1
                or metrics['queue_overflow_count'] != len(events)
                or metrics['frames_enqueued'] >= frames
                or 'Consumer connection 1: queue overflow' not in processes['server'].lines):
            raise ValueError('Expected isolated SLOW overflow was not observed')
        # At the decision point the current incoming frame was not enqueued.
        last_sequence = (metrics['frames_enqueued'] + 1)
        if any(s['frames_enqueued'] > metrics['frames_enqueued'] for s in record['samples']
               if s['consumer_connection'] == 1):
            raise ValueError('SLOW continued enqueue after disconnect')
        errors = diagnostic_lines(slow.lines)
        if slow.child.returncode == 0:
            if errors or 'Stream ended normally' not in slow.lines or not 0 < record['consumers']['1'].get('frames_received', 0) < frames:
                raise ValueError('Invalid SLOW clean EOF outcome')
            outcome = 'clean EOF after isolated overflow'
        elif slow.child.returncode == 1:
            if len(errors) != 1 or not expected_receive_termination(errors[0]):
                raise ValueError('SLOW failure is not expected transport termination')
            outcome = errors[0]
        else:
            raise ValueError(f'SLOW crashed/unexpected exit {slow.child.returncode}')
        if 'Payload pattern valid' not in slow.lines:
            raise ValueError('No validated SLOW stream prefix observed')
        record['slow_outcome'] = {'expected_policy_disconnect': True,
                                  'exit_code': slow.child.returncode,
                                  'transport_outcome': outcome,
                                  'frames_received': record['consumers']['1'].get('frames_received'),
                                  'overflow_incoming_sequence': last_sequence,
                                  'note': 'A partial-Frame EOF may exit 1 without a final Consumer summary; missing frame counts are not inferred.'}
    elif record['events']:
        raise ValueError('FAST/FAST unexpectedly overflowed')


async def run_one(tree, out, scenario, frames, definitions, used_ports):
    primary = scenario == 'SLOW/FAST'
    name = 'slow-fast-1800' if primary else 'fast-fast-300'
    log_dir = out / 'logs' / name
    log_dir.mkdir(parents=True)
    processes = {}
    record = {'scenario': scenario, 'frames_target': frames, 'samples': [], 'events': []}

    async def launch(role, executable, args):
        command = ['stdbuf', '-oL', '-eL', str(tree / BUILD / executable), *args]
        child = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        proc = common.Process(role, child, command, log_dir / f'{role}.log')
        processes[role] = proc
        return proc

    try:
        producer_port, consumer_port = common.choose_ports(used_ports)
        print(f'START {name}', flush=True)
        server = await launch('server', 'esl_server', [
            '--producer-port', str(producer_port), '--consumer-port', str(consumer_port),
            '--expected-consumers', '2'])
        await server.wait_line('Producer connection waiting...')
        mode = 'slow' if primary else 'fast'
        args = ['--host', '127.0.0.1', '--port', str(consumer_port), '--mode', mode, '--id', '10']
        if primary:
            args += ['--delay-ms', '100']
        first = await launch('consumer1', 'esl_consumer', args)
        await first.wait_line('Connected to server')
        second = await launch('consumer2', 'esl_consumer', [
            '--host', '127.0.0.1', '--port', str(consumer_port), '--mode', 'fast', '--id', '1'])
        await second.wait_line('Connected to server')
        t0 = time.monotonic()
        record['producer_launch_t0_monotonic_s'] = t0
        await launch('producer', 'esl_producer', [
            '--host', '127.0.0.1', '--port', str(producer_port), '--fps', '30', '--frames', str(frames)])
        watchers = asyncio.gather(*(p.watcher for p in processes.values()))
        await asyncio.wait_for(asyncio.shield(watchers), timeout=180 if primary else 90)
        await asyncio.gather(*(p.reader for p in processes.values()))
        record['completion'] = {f'{role}_completion_s': p.completion - t0 for role, p in processes.items()}
        record['producer'] = common.parse_values(processes['producer'].lines)
        record['consumers'] = {str(i): common.parse_values(processes[f'consumer{i}'].lines) for i in (1, 2)}
        record['server'] = common.parse_server(server.lines)
        record['samples'] = sampling.parse_samples(server.lines, frames)
        record['events'] = parse_events(server.lines)
        large = frames // definitions['large_interval']
        expected_bytes = ((frames-large)*definitions['normal_payload_bytes']
                          + large*definitions['large_payload_bytes'] + frames*definitions['wire_header_bytes'])
        record['expected_wire_bytes_per_consumer'] = expected_bytes
        validate(record, processes, frames, expected_bytes, primary)
        record['correctness'] = 'PASS'
        print(f"DONE {name}: {json.dumps(record['completion'])} events={json.dumps(record['events'])}", flush=True)
    except Exception as exc:
        record['correctness'] = 'FAIL'
        record['error'] = repr(exc)
        print(f'FAIL {name}: {exc}', flush=True)
    finally:
        for proc in processes.values():
            if proc.child.returncode is None:
                try:
                    proc.child.terminate()
                except ProcessLookupError:
                    pass
        for proc in processes.values():
            try:
                await asyncio.wait_for(proc.child.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    proc.child.kill()
                except ProcessLookupError:
                    pass
                await proc.child.wait()
        await asyncio.gather(*(p.reader for p in processes.values()), return_exceptions=True)
        await asyncio.gather(*(p.watcher for p in processes.values()), return_exceptions=True)
        record['commands'] = {role: p.command for role, p in processes.items()}
        record['exit_codes'] = {role: p.child.returncode for role, p in processes.items()}
        if 'server' in processes:
            record['samples'] = sampling.parse_samples(processes['server'].lines, frames)
            record['events'] = parse_events(processes['server'].lines)
        with (out / 'samples' / f'{name}.csv').open('w', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=('frames_target', *sampling.SAMPLE_FIELDS))
            writer.writeheader()
            writer.writerows(record['samples'])
    return record


def write_results(out, document):
    rows, events, details = [], [], []
    lines = ['# EventStreamLab Day 7 — Bounded TX Queue and Overflow Isolation', '',
             '## A. Policy', '',
             f'Per-Consumer logical unsent TX backlog limit: 32 MiB ({LIMIT} bytes). Check before enqueue; disconnect only the overloaded Consumer. This isolates Producer/FAST without silent Frame dropping, and is not a universal policy. Shared Frames, allocator overhead and kernel buffers mean process RSS is not capped at 32 MiB.', '',
             'Linux Debug, 30 FPS; primary SLOW #1 100 ms / FAST #2, 1800 Frames; control FAST/FAST, 300 Frames. Completion seconds start immediately before Producer launch; event/sample elapsed_ms starts with the Server event loop. No performance threshold.', '',
             '## B. Primary and C. Control results', '',
             '| Scenario | Correctness | Producer Frames | FAST #2 Frames | Producer s | FAST #2 s | Server s | #1 exact peak bytes | Overflow events |',
             '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for run in document['runs']:
        row = {'scenario': run['scenario'], 'frames_target': run['frames_target'], 'correctness': run['correctness']}
        row.update(run.get('completion', {}))
        events.extend({'scenario': run['scenario'], 'frames_target': run['frames_target'], **e} for e in run['events'])
        if 'server' in run:
            metrics = run['server']['connections']
            row.update(producer_frames=run['producer'].get('frames_sent'),
                       fast_frames=run['consumers']['2'].get('frames_received'),
                       consumer1_frames=run['consumers']['1'].get('frames_received'))
            for i in ('1','2'):
                for key in ('frames_enqueued','frames_completed','max_pending_frames','max_pending_bytes','queue_overflow_count','disconnected_due_to_overflow'):
                    row[f'consumer{i}_{key}'] = metrics[i][key]
            p = run['completion']
            lines.append(f"| {run['scenario']} | {run['correctness']} | {row['producer_frames']} | {row['fast_frames']} | {p['producer_completion_s']:.3f} | {p['consumer2_completion_s']:.3f} | {p['server_completion_s']:.3f} | {metrics['1']['max_pending_bytes']} | {len(run['events'])} |")
            for e in run['events']:
                details += ['', f"Overflow at {e['elapsed_ms']} ms: Consumer #{e['consumer_connection']}, {e['pending_frames']} pending Frames / {e['pending_bytes']} pending bytes, incoming {e['incoming_frame_bytes']} bytes, limit {e['limit_bytes']}, policy {e['policy']}."]
            if 'slow_outcome' in run:
                details += ['', f"SLOW outcome: {json.dumps(run['slow_outcome'])}. Server SLOW counters remain after queue clear; no new enqueue occurs after invalidating its socket."]
        rows.append(row)
    lines += details
    passed = all(r['correctness']=='PASS' for r in document['runs']) and len(document['runs'])==2
    lines += ['', '## D. Interpretation', '']
    if passed:
        lines.append('Both correctness checks passed. Day 6 unbounded SLOW peak at 1800 Frames was 136,110,018 bytes. Day 7 enforces bounded logical backlog and explicitly isolates the overflowing connection; Producer and FAST completed all 1800 Frames. Compare the measured completion times above without a performance pass threshold. The FAST/FAST control had no overflow. Exact peaks are updated on every enqueue, while ~1 s samples may miss instantaneous peaks.')
    else:
        lines.append('Run correctness is incomplete or failed; inspect raw_runs.json and process logs. No isolation conclusion is assumed.')
    lines += ['', '## E. Tradeoff', '',
              '- Upstream backpressure: can suit lossless delivery, but SLOW can affect overall latency.',
              '- DROP_NEWEST: retains backlog and loses newest information.',
              '- DROP_OLDEST: favors current state, but requires sequence-gap/protocol semantics.',
              '- Disconnect (selected): isolates other connections and gives up SLOW stream continuity.', '',
              'Lossless/reliable domains may need a different policy. No Day 8 Release benchmark is included.', '',
              'Artifacts: raw_runs.json, events.csv, samples/ and logs/. A SLOW exit 1 is accepted only for narrowly recognized transport termination corroborated by Server overflow metrics; validation failures and crashes remain failures.']
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with (out/'summary.csv').open('w', newline='') as output:
        writer=csv.DictWriter(output,fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    with (out/'events.csv').open('w', newline='') as output:
        writer=csv.DictWriter(output,fieldnames=('scenario','frames_target',*EVENT_FIELDS))
        writer.writeheader()
        writer.writerows(events)
    (out/'summary.md').write_text('\n'.join(lines)+'\n')
    (out/'raw_runs.json').write_text(json.dumps(document,indent=2)+'\n')


async def main(args):
    if platform.system() != 'Linux':
        raise ValueError('Linux is required')
    tree, out = args.event_tree.resolve(), args.out.resolve()
    if out.exists() and any(out.iterdir()):
        raise ValueError('Refusing to overwrite existing results')
    cache = (tree/BUILD/'CMakeCache.txt').read_text()
    if not re.search(r'^CMAKE_BUILD_TYPE:STRING=Debug$',cache,re.M):
        raise ValueError('Day 7 Debug build required')
    metadata = {
        'commit': common.command_output(['git','rev-parse','HEAD'],tree),
        'tracked_status': common.tracked_status(tree),
        'server_diff': common.command_output(['git','diff','--','src/server/main.cpp'],tree),
        'platform': platform.platform(), 'build_type':'Debug', 'queue_limit_bytes':LIMIT,
        'policy':'disconnect', 'fps':30, 'slow_delay_ms':100,
        'compiler':common.command_output(['g++','--version']).splitlines()[0],
        'cmake':common.command_output(['cmake','--version']).splitlines()[0],
        'source_hashes':{p:hashlib.sha256((tree/p).read_bytes()).hexdigest() for p in (
            'src/server/main.cpp','src/producer/main.cpp','src/consumer/main.cpp',
            'include/eventstream/workload.hpp','include/eventstream/protocol.hpp')},
        'runner_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
    }
    definitions=common.workload(tree)
    metadata['workload']=definitions
    out.mkdir(parents=True,exist_ok=True)
    (out/'samples').mkdir()
    document={'metadata':metadata,'runs':[]}
    used_ports=set()
    for scenario,frames in (('SLOW/FAST',1800),('FAST/FAST',300)):
        record=await run_one(tree,out,scenario,frames,definitions,used_ports)
        document['runs'].append(record)
        write_results(out,document)
        if record['correctness']!='PASS':
            raise RuntimeError('Policy/correctness failure; stopping')
    print(f'Both runs passed; results: {out}',flush=True)


if __name__=='__main__':
    asyncio.run(main(parse_args()))
