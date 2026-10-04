"""Validate the final Release server under normal, tolerated and overload load.

Use independent asyncio child completion watchers, retain queue/policy samples,
and summarize seven fixed workloads without performance pass thresholds.
"""

import argparse
import asyncio
import csv
import hashlib
import json
import pathlib
import platform
import re
import statistics
import time

import day5_compare as common
import day6_queue_growth as sampling
import day7_bounded_queue as policy

BUILD = pathlib.Path('out/day8-release')
LIMIT = 32 * 1024 * 1024
NORMAL = 'FAST/FAST'
UNDER = 'SLOW/FAST under-limit'
OVER = 'SLOW/FAST overload'
PLAN = [(scenario, run, 1800) for run in (1, 2, 3) for scenario in (NORMAL, OVER)] + [(UNDER, 1, 300)]
METRIC_KEYS = ('frames_enqueued', 'frames_completed', 'frames_forwarded', 'bytes_sent',
               'max_pending_frames', 'max_pending_bytes', 'queue_overflow_count',
               'disconnected_due_to_overflow', 'eagain_count', 'epollout_wakes', 'partial_writes')
COMPLETIONS = ('producer_completion_s', 'consumer1_completion_s', 'consumer2_completion_s', 'server_completion_s')


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event-tree', type=pathlib.Path, default=pathlib.Path('.'))
    parser.add_argument('--out', type=pathlib.Path, default=pathlib.Path('/tmp/eventstreamlab-day8'))
    return parser.parse_args()


def run_name(scenario, number):
    if scenario == NORMAL:
        return f'fast-fast-1800-run{number}'
    if scenario == OVER:
        return f'slow-fast-overload-1800-run{number}'
    return 'slow-fast-under-limit-300'


def source_hashes(tree):
    files = common.command_output(['git', 'ls-files', 'src', 'include', 'CMakeLists.txt'], tree).splitlines()
    return {p: hashlib.sha256((tree / p).read_bytes()).hexdigest() for p in files}


def metadata(tree):
    if common.tracked_status(tree):
        raise ValueError('Tracked source tree must be clean')
    if common.command_output(['git', 'branch', '--show-current'], tree) != 'main':
        raise ValueError('Expected main branch')
    cache_path = tree / BUILD / 'CMakeCache.txt'
    cache = cache_path.read_text()

    def cache_value(key):
        match = re.search(r'^' + re.escape(key) + r':[^=]+=(.*)$', cache, re.M)
        if match is None:
            raise ValueError(f'Missing CMake cache value: {key}')
        return match.group(1)

    if cache_value('CMAKE_BUILD_TYPE') != 'Release':
        raise ValueError('Release build required')
    if pathlib.Path(cache_value('CMAKE_HOME_DIRECTORY')).resolve() != tree:
        raise ValueError('Build cache belongs to a different source tree')
    compiler = cache_value('CMAKE_CXX_COMPILER')
    return {
        'date_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'commit': common.command_output(['git', 'rev-parse', 'HEAD'], tree),
        'branch': 'main', 'tracked_status_before': common.tracked_status(tree),
        'build_type': 'Release', 'build_directory': str(tree / BUILD),
        'compiler': common.command_output([compiler, '--version']).splitlines()[0],
        'compiler_path': compiler,
        'cmake': common.command_output(['cmake', '--version']).splitlines()[0],
        'kernel': common.command_output(['uname', '-a']), 'platform': platform.platform(),
        'CMAKE_CXX_FLAGS_RELEASE': cache_value('CMAKE_CXX_FLAGS_RELEASE'),
        'CMAKE_CXX_FLAGS': cache_value('CMAKE_CXX_FLAGS'),
        'cmake_cache_sha256': hashlib.sha256(cache_path.read_bytes()).hexdigest(),
        'binary_hashes': {p: hashlib.sha256((tree / BUILD / p).read_bytes()).hexdigest()
                          for p in ('esl_server', 'esl_producer', 'esl_consumer')},
        'source_hashes': source_hashes(tree),
        'runner_sha256': hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
        'fps': 30, 'slow_delay_ms': 100, 'queue_limit_bytes': LIMIT, 'policy': 'disconnect',
        'workload': {k: v for k, v in common.workload(tree).items()
                     if k != 'expected_wire_bytes_per_consumer'},
        'timeouts_s': {NORMAL: 120, OVER: 120, UNDER: 90},
        'plan': [{'scenario': s, 'run': r, 'frames': f} for s, r, f in PLAN],
        'primary_clock': 'independent process completion monotonic timestamp minus timestamp immediately before Producer launch',
        'queue_clock': 'Server event loop start; separate origin from primary clock',
    }


def check_provenance(tree, info):
    if (common.tracked_status(tree)
            or common.command_output(['git', 'rev-parse', 'HEAD'], tree) != info['commit']
            or source_hashes(tree) != info['source_hashes']):
        raise ValueError('Application source changed during measurement')
    for name, digest in info['binary_hashes'].items():
        if hashlib.sha256((tree / BUILD / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Measured binary changed')


def validate_normal(record, processes):
    frames = record['frames_target']
    if (record['producer'].get('frames_sent') != frames
            or record['server']['summary'].get('frames_received') != frames):
        raise ValueError('Producer/Server frame count mismatch')
    if record['events']:
        raise ValueError('Unexpected overflow in normal/under-limit scenario')
    for role, p in processes.items():
        if p.child.returncode != 0 or policy.diagnostic_lines(p.lines):
            raise ValueError(f'{role} process/validation failure')
    for i in ('1', '2'):
        metrics = record['server']['connections'][i]
        consumer = record['consumers'][i]
        mode = 'slow' if i == '1' and record['scenario'] == UNDER else 'fast'
        if (consumer.get('frames_received') != frames or consumer.get('mode') != mode
                or consumer.get('id') != (10 if i == '1' else 1)
                or any(metrics[k] != frames for k in ('frames_enqueued', 'frames_completed', 'frames_forwarded'))
                or metrics['bytes_sent'] != record['expected_wire_bytes_per_consumer']
                or metrics['queue_overflow_count'] != 0 or metrics['disconnected_due_to_overflow'] != 0
                or not 0 <= metrics['max_pending_bytes'] <= LIMIT):
            raise ValueError(f'Consumer {i} correctness/queue mismatch')
        group = [s for s in record['samples'] if s['consumer_connection'] == int(i)]
        if (not group or group[0]['pending_bytes'] != 0 or group[0]['pending_frames'] != 0
                or group[-1]['pending_bytes'] != 0 or group[-1]['pending_frames'] != 0
                or group[-1]['producer_finished'] != 1 or group[-1]['frames_completed'] != frames):
            raise ValueError(f'Consumer {i} missing complete queue lifecycle')
    for s in record['samples']:
        if (not 0 <= s['pending_bytes'] <= LIMIT
                or s['pending_frames'] != s['frames_enqueued'] - s['frames_completed']):
            raise ValueError('Queue sample invariant violation')


async def run_one(tree, out, scenario, number, frames, info, used_ports):
    name = run_name(scenario, number)
    log_dir = out / 'logs' / name
    log_dir.mkdir(parents=True)
    record = {'scenario': scenario, 'run': number, 'frames_target': frames,
              'name': name, 'samples': [], 'events': [], 'timeout_s': info['timeouts_s'][scenario]}
    processes = {}

    async def launch(role, executable, arguments):
        command = ['stdbuf', '-oL', '-eL', str(tree / BUILD / executable), *arguments]
        child = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        p = common.Process(role, child, command, log_dir / f'{role}.log')
        processes[role] = p
        return p

    try:
        check_provenance(tree, info)
        producer_port, consumer_port = common.choose_ports(used_ports)
        record.update(producer_port=producer_port, consumer_port=consumer_port)
        print(f'START {name}', flush=True)
        server = await launch('server', 'esl_server', [
            '--producer-port', str(producer_port), '--consumer-port', str(consumer_port), '--expected-consumers', '2'])
        await server.wait_line('Producer connection waiting...')
        mode = 'fast' if scenario == NORMAL else 'slow'
        args = ['--host', '127.0.0.1', '--port', str(consumer_port), '--mode', mode, '--id', '10']
        if mode == 'slow':
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
        monitors = asyncio.gather(*(p.watcher for p in processes.values()))
        await asyncio.wait_for(asyncio.shield(monitors), timeout=record['timeout_s'])
        await asyncio.gather(*(p.reader for p in processes.values()))
        record['completion'] = {f'{role}_completion_s': p.completion - t0 for role, p in processes.items()}
        aliases = ('fast1_completion_s', 'fast2_completion_s') if scenario == NORMAL else ('slow_completion_s', 'fast_completion_s')
        record['completion'].update(zip(aliases, (record['completion']['consumer1_completion_s'], record['completion']['consumer2_completion_s'])))
        record['producer'] = common.parse_values(processes['producer'].lines)
        record['consumers'] = {str(i): common.parse_values(processes[f'consumer{i}'].lines) for i in (1, 2)}
        record['server'] = common.parse_server(server.lines)
        record['samples'] = sampling.parse_samples(server.lines, frames)
        record['events'] = policy.parse_events(server.lines)
        definitions = info['workload']
        large = frames // definitions['large_interval']
        record['expected_wire_bytes_per_consumer'] = (
            (frames-large)*definitions['normal_payload_bytes'] + large*definitions['large_payload_bytes']
            + frames*definitions['wire_header_bytes'])
        if scenario == OVER:
            policy.validate(record, processes, frames, record['expected_wire_bytes_per_consumer'], True)
        else:
            validate_normal(record, processes)
        if scenario == UNDER:
            record['slow_drain'] = sampling.queue_metrics(record['samples'], 1)
        record['correctness'] = 'PASS'
        print(f"DONE {name}: {json.dumps(record['completion'])} events={json.dumps(record['events'])}", flush=True)
    except Exception as exc:
        record['correctness'] = 'FAIL'
        record['error'] = repr(exc)
        print(f'FAIL {name}: {exc}', flush=True)
    finally:
        for p in processes.values():
            if p.child.returncode is None:
                try:
                    p.child.terminate()
                except ProcessLookupError:
                    pass
        for p in processes.values():
            try:
                await asyncio.wait_for(p.child.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    p.child.kill()
                except ProcessLookupError:
                    pass
                await p.child.wait()
        await asyncio.gather(*(p.reader for p in processes.values()), return_exceptions=True)
        await asyncio.gather(*(p.watcher for p in processes.values()), return_exceptions=True)
        record['commands'] = {role: p.command for role, p in processes.items()}
        record['exit_codes'] = {role: p.child.returncode for role, p in processes.items()}
        record['diagnostics'] = {role: policy.diagnostic_lines(p.lines) for role, p in processes.items()}
        if 'server' in processes:
            record['samples'] = sampling.parse_samples(processes['server'].lines, frames)
            record['events'] = policy.parse_events(processes['server'].lines)
        with (out / 'samples' / f'{name}.csv').open('w', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=('frames_target', *sampling.SAMPLE_FIELDS))
            writer.writeheader()
            writer.writerows(record['samples'])
    return record


def stats(values):
    return {'median': statistics.median(values), 'min': min(values), 'max': max(values)}


def write_results(out, document):
    runs = document['runs']
    successful = [r for r in runs if r['correctness'] == 'PASS']
    rows, events = [], []
    for r in runs:
        row = {k: r[k] for k in ('scenario', 'run', 'frames_target', 'correctness')}
        row.update(r.get('completion', {}))
        row['producer_frames_sent'] = r.get('producer', {}).get('frames_sent')
        row['server_frames_received'] = r.get('server', {}).get('summary', {}).get('frames_received')
        for i in ('1', '2'):
            row[f'consumer{i}_frames_received'] = r.get('consumers', {}).get(i, {}).get('frames_received')
            row[f'consumer{i}_exit_code'] = r.get('exit_codes', {}).get(f'consumer{i}')
            m = r.get('server', {}).get('connections', {}).get(i, {})
            row.update({f'consumer{i}_{key}': m.get(key) for key in METRIC_KEYS})
        rows.append(row)
        events.extend({'scenario': r['scenario'], 'run': r['run'], 'frames_target': r['frames_target'], **e} for e in r['events'])
    columns = list(dict.fromkeys(k for row in rows for k in row))
    with (out / 'summary.csv').open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    with (out / 'events.csv').open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=('scenario', 'run', 'frames_target', *policy.EVENT_FIELDS))
        writer.writeheader()
        writer.writerows(events)
    meta = document['metadata']
    lines = ['# EventStreamLab Day 8 — Final Release Benchmark', '', '## A. Environment', '',
             f"- Platform: {meta['platform']}", f"- Kernel: {meta['kernel']}",
             '- Build: Release', f"- Compiler: {meta['compiler']} ({meta['compiler_path']})",
             f"- CMake: {meta['cmake']}", f"- Commit: `{meta['commit']}`",
             f"- CMAKE_CXX_FLAGS_RELEASE: `{meta['CMAKE_CXX_FLAGS_RELEASE']}`",
             f"- CMAKE_CXX_FLAGS: `{meta['CMAKE_CXX_FLAGS']}`",
             '- Policy: 32 MiB logical unsent bytes per Consumer, disconnect on overflow.', '',
             '30 FPS, FAST #2; Consumer #1 FAST or SLOW 100 ms. Independent process completion seconds start immediately before Producer launch; sample/event milliseconds start with the Server event loop. No performance pass threshold.']
    document['statistics'] = {}
    document['queue_statistics'] = {}
    for scenario, heading in ((NORMAL, '## B. Scenario A — FAST/FAST 1800'),
                              (UNDER, '## C. Scenario B — SLOW/FAST 300 under-limit'),
                              (OVER, '## D. Scenario C — SLOW/FAST 1800 overload')):
        group = [r for r in successful if r['scenario'] == scenario]
        lines += ['', heading, '',
                  '| Run | Producer s | Consumer #1 s | FAST #2 s | Server s | #1 exact peak frames | #1 exact peak bytes | #2 exact peak bytes | #1 overflow |',
                  '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
        for r in group:
            c = r['completion']
            m1, m2 = r['server']['connections']['1'], r['server']['connections']['2']
            lines.append(f"| {r['run']} | {c['producer_completion_s']:.3f} | {c['consumer1_completion_s']:.3f} | {c['consumer2_completion_s']:.3f} | {c['server_completion_s']:.3f} | {m1['max_pending_frames']} | {m1['max_pending_bytes']} | {m2['max_pending_bytes']} | {m1['queue_overflow_count']} |")
        if scenario in (NORMAL, OVER) and len(group) == 3:
            completion_stats = {key: stats([r['completion'][key] for r in group]) for key in COMPLETIONS}
            document['statistics'][scenario] = completion_stats
            document['queue_statistics'][scenario] = {
                i: {key: stats([r['server']['connections'][i][key] for r in group])
                    for key in ('max_pending_frames', 'max_pending_bytes')}
                for i in ('1', '2')}
            lines += ['', '| Metric | Median s | Min s | Max s |', '| --- | ---: | ---: | ---: |']
            for key, values in completion_stats.items():
                lines.append(f"| {key} | {values['median']:.3f} | {values['min']:.3f} | {values['max']:.3f} |")
        if scenario == UNDER and group:
            r = group[0]
            q = r['slow_drain']
            lines += ['', f"All processes exited 0 and completed {r['frames_target']} Frames. Both overflow/disconnect metrics are 0. SLOW backlog at EOF: {q['at_eof_pending_frames']} Frames / {q['at_eof_pending_bytes']} bytes; final zero after {q['drain_after_eof_s']:.3f} s of TX drain. A Slow Consumer within configured capacity is served through complete drain."]
        if scenario == OVER:
            lines += ['', '| Run | Overflow ms | Consumer | Pending bytes | Incoming bytes | Limit bytes | FAST completed | FAST overflow |',
                      '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
            for r in group:
                m = r['server']['connections']['2']
                for e in r['events']:
                    lines.append(f"| {r['run']} | {e['elapsed_ms']} | {e['consumer_connection']} | {e['pending_bytes']} | {e['incoming_frame_bytes']} | {e['limit_bytes']} | {m['frames_completed']} | {m['queue_overflow_count']} |")
            for r in group:
                lines += ['', f"Run {r['run']} SLOW policy outcome: {json.dumps(r['slow_outcome'])}"]
    complete = len(successful) == 7
    lines += ['', '## E. Final interpretation', '']
    if complete:
        lines += ['All seven correctness checks passed. Normal Consumers did not trigger the bounded queue policy. Under-limit SLOW backlog was tolerated and drained. Sustained overload isolated only the SLOW connection before its logical backlog would exceed 32 MiB; Producer and FAST completed all 1800 Frames after isolation.', '',
                  'The limit applies to per-Consumer logical unsent TX backlog, not process RSS. Shared Frame storage, allocator overhead and kernel socket buffers are separate. Exact peaks track every enqueue; coarse samples can miss instantaneous maxima.']
    else:
        lines.append('Measurement is incomplete or failed; no final success conclusion is assumed. Inspect raw_runs.json and logs for the recorded failure.')
    lines += ['', 'Day 5 is the Blocking versus Event architecture comparison. Day 8 validates the final Release implementation; no new Blocking speedup comparison is made because overload semantics differ.', '',
              '## F. Caveats', '',
              '- Linux / WSL loopback, synthetic workload, one Producer and two Consumers; not production network hardware.',
              '- Immediate overload disconnect may terminate SLOW mid-Frame. Exit 1 is accepted only for corroborated EOF/reset transport termination; validation failures and crashes fail correctness.',
              '- Policy disconnect gives up SLOW stream continuity; missing final SLOW Frame counts are not inferred.',
              '- Existing ~1 s queue instrumentation adds observation overhead.',
              '- Completion values are observations, not universal throughput claims.', '',
              f'Correctness: {len(successful)}/{len(runs)} recorded runs passed. Full summaries/metadata: raw_runs.json; overflow events: events.csv; queue samples: samples/; process logs and timestamped timelines: logs/.']
    (out / 'summary.md').write_text('\n'.join(lines) + '\n')
    (out / 'raw_runs.json').write_text(json.dumps(document, indent=2) + '\n')


async def main(args):
    if platform.system() != 'Linux':
        raise ValueError('Linux is required')
    tree, out = args.event_tree.resolve(), args.out.resolve()
    if out.exists():
        raise ValueError('Output already exists; refusing overwrite')
    info = metadata(tree)
    out.mkdir(parents=True)
    (out / 'samples').mkdir()
    document = {'metadata': info, 'runs': []}
    used_ports = set()
    for scenario, number, frames in PLAN:
        record = await run_one(tree, out, scenario, number, frames, info, used_ports)
        document['runs'].append(record)
        write_results(out, document)
        if record['correctness'] != 'PASS':
            raise RuntimeError('Correctness/orchestration failure; stopped')
    check_provenance(tree, info)
    info['tracked_status_after'] = common.tracked_status(tree)
    info['source_hashes_after'] = source_hashes(tree)
    info['provenance_verified_after'] = True
    write_results(out, document)
    print(f'All seven Release runs passed; results: {out}', flush=True)


if __name__ == '__main__':
    asyncio.run(main(parse_args()))
