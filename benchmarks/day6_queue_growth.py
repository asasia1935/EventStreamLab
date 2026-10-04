"""Observe unbounded Linux Debug Consumer TX queues at three workload lengths.

Reuse Day 5 process capture and independent completion watchers; record queue
samples through Producer EOF and complete Consumer drain. No queue policy.
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

BUILD = pathlib.Path('out/day6-debug')
LENGTHS = (300, 900, 1800)
TIMEOUTS = {300: 90, 900: 180, 1800: 300}
SAMPLE_FIELDS = ('elapsed_ms', 'producer_finished', 'frames_received',
                 'consumer_connection', 'pending_frames', 'pending_bytes',
                 'front_offset', 'frames_enqueued', 'frames_completed')


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event-tree', type=pathlib.Path, default=pathlib.Path('.'))
    parser.add_argument('--out', type=pathlib.Path, default=pathlib.Path('/tmp/eventstreamlab-day6'))
    return parser.parse_args()


def parse_samples(lines, frames):
    samples = []
    for line in lines:
        if line.startswith('queue_sample '):
            fields = dict(part.split('=', 1) for part in line.split()[1:])
            if set(fields) != set(SAMPLE_FIELDS):
                raise ValueError(f'Malformed queue sample: {line}')
            sample = {key: int(fields[key]) for key in SAMPLE_FIELDS}
            samples.append({'frames_target': frames, **sample})
    return samples


def queue_metrics(samples, connection):
    group = [s for s in samples if s['consumer_connection'] == connection]
    if not group:
        raise ValueError(f'Missing samples for Consumer {connection}')
    peak = max(group, key=lambda s: s['pending_bytes'])
    eof = next((s for s in group if s['producer_finished']), None)
    last = group[-1]
    if (group[0]['pending_frames'] != 0 or group[0]['pending_bytes'] != 0
            or eof is None or last['pending_frames'] != 0 or last['pending_bytes'] != 0):
        raise ValueError(f'Incomplete queue sampling lifecycle for Consumer {connection}')
    return {
        'sample_peak_frames': max(s['pending_frames'] for s in group),
        'sample_peak_bytes': peak['pending_bytes'],
        'sample_peak_elapsed_ms': peak['elapsed_ms'],
        'at_eof_pending_frames': eof['pending_frames'],
        'at_eof_pending_bytes': eof['pending_bytes'],
        'eof_elapsed_ms': eof['elapsed_ms'],
        'final_zero_elapsed_ms': last['elapsed_ms'],
        'drain_after_eof_s': (last['elapsed_ms'] - eof['elapsed_ms']) / 1000,
    }


async def run_one(tree, out, frames, definitions, ports_used):
    logs = out / 'logs' / f'frames-{frames}'
    logs.mkdir(parents=True)
    processes = {}
    record = {'frames_target': frames, 'timeout_s': TIMEOUTS[frames]}

    async def launch(role, executable, args):
        command = ['stdbuf', '-oL', '-eL', str(tree / BUILD / executable), *args]
        child = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        proc = common.Process(role, child, command, logs / f'{role}.log')
        processes[role] = proc
        return proc

    try:
        producer_port, consumer_port = common.choose_ports(ports_used)
        print(f'START frames={frames}', flush=True)
        server = await launch('server', 'esl_server', [
            '--producer-port', str(producer_port), '--consumer-port', str(consumer_port),
            '--expected-consumers', '2'])
        await server.wait_line('Producer connection waiting...')
        slow = await launch('consumer1', 'esl_consumer', [
            '--host', '127.0.0.1', '--port', str(consumer_port), '--mode', 'slow',
            '--delay-ms', '100', '--id', '10'])
        await slow.wait_line('Connected to server')
        fast = await launch('consumer2', 'esl_consumer', [
            '--host', '127.0.0.1', '--port', str(consumer_port), '--mode', 'fast', '--id', '1'])
        await fast.wait_line('Connected to server')
        t0 = time.monotonic()
        record['producer_launch_t0_monotonic_s'] = t0
        await launch('producer', 'esl_producer', [
            '--host', '127.0.0.1', '--port', str(producer_port), '--fps', '30', '--frames', str(frames)])
        watchers = asyncio.gather(*(p.watcher for p in processes.values()))
        await asyncio.wait_for(asyncio.shield(watchers), timeout=TIMEOUTS[frames])
        await asyncio.gather(*(p.reader for p in processes.values()))
        record['primary'] = {f'{role}_completion_s': p.completion - t0 for role, p in processes.items()}
        record['primary']['slow_completion_s'] = record['primary']['consumer1_completion_s']
        record['primary']['fast_completion_s'] = record['primary']['consumer2_completion_s']
        record['producer'] = common.parse_values(processes['producer'].lines)
        record['consumers'] = {str(i): common.parse_values(processes[f'consumer{i}'].lines) for i in (1, 2)}
        record['server'] = common.parse_server(server.lines)
        large = frames // definitions['large_interval']
        expected_bytes = ((frames - large) * definitions['normal_payload_bytes']
                          + large * definitions['large_payload_bytes']
                          + frames * definitions['wire_header_bytes'])
        record['expected_wire_bytes_per_consumer'] = expected_bytes
        for role, proc in processes.items():
            if proc.child.returncode != 0:
                raise ValueError(f'{role} exited {proc.child.returncode}')
            if any(re.search(r'failed|mismatch|rejected|incomplete|unexpected|error', line, re.I) for line in proc.lines):
                raise ValueError(f'{role} reported an error')
        if record['producer']['frames_sent'] != frames or record['server']['summary']['frames_received'] != frames:
            raise ValueError('Producer/Server frame count mismatch')
        for i in ('1', '2'):
            consumer = record['consumers'][i]
            metrics = record['server']['connections'][i]
            if (consumer['frames_received'] != frames
                    or consumer['mode'] != ('slow' if i == '1' else 'fast')
                    or any(metrics[k] != frames for k in ('frames_enqueued', 'frames_completed', 'frames_forwarded'))
                    or metrics['bytes_sent'] != expected_bytes):
                raise ValueError(f'Consumer {i} correctness mismatch')
        samples = parse_samples(server.lines, frames)
        record['queues'] = {str(i): queue_metrics(samples, i) for i in (1, 2)}
        for i in ('1', '2'):
            record['queues'][i].update({
                'exact_peak_frames': record['server']['connections'][i]['max_pending_frames'],
                'exact_peak_bytes': record['server']['connections'][i]['max_pending_bytes']})
        record['samples'] = samples
        record['correctness'] = 'PASS'
        print(f"DONE frames={frames} {json.dumps(record['primary'])} queues={json.dumps(record['queues'])}", flush=True)
    except Exception as exc:
        record['correctness'] = 'FAIL'
        record['error'] = repr(exc)
        print(f'FAIL frames={frames}: {exc}', flush=True)
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
        if 'samples' not in record and 'server' in processes:
            record['samples'] = parse_samples(processes['server'].lines, frames)
        samples_path = out / 'samples' / f'frames-{frames}.csv'
        with samples_path.open('w', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=('frames_target', *SAMPLE_FIELDS))
            writer.writeheader()
            writer.writerows(record.get('samples', []))
    return record


def write_results(out, document):
    rows = []
    lines = ['# EventStreamLab Day 6 — Unbounded TX Queue Growth Observation', '',
             '## A. Experiment conditions', '',
             'Linux Debug, 30 FPS, SLOW #1 100 ms delay, FAST #2; sequential 300 / 900 / 1800 Frames. Independent process completion is measured from immediately before Producer launch. Coarse ~1 s logging adds observation overhead.', '',
             'Queue samples use event-loop elapsed_ms, a separate origin from Producer launch. Pending bytes are logical unsent wire bytes, not RSS. Exact peaks use existing Server summary counters; sampled peaks can miss short-lived maxima.', '',
             '## B. Summary', '',
             '| Frames | Producer s | FAST s | SLOW s | Server s | SLOW peak frames | SLOW peak bytes | FAST peak frames | FAST peak bytes |',
             '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for run in document['runs']:
        row = {'frames_target': run['frames_target'], 'correctness': run['correctness']}
        if run['correctness'] == 'PASS':
            p, slow, fast = run['primary'], run['queues']['1'], run['queues']['2']
            row.update(p)
            row.update({f'{label}_{key}': value for label, metrics in (('slow', slow), ('fast', fast)) for key, value in metrics.items()})
            lines.append(f"| {run['frames_target']} | {p['producer_completion_s']:.3f} | {p['fast_completion_s']:.3f} | {p['slow_completion_s']:.3f} | {p['server_completion_s']:.3f} | {slow['exact_peak_frames']} | {slow['exact_peak_bytes']} | {fast['exact_peak_frames']} | {fast['exact_peak_bytes']} |")
        rows.append(row)
    lines += ['', '## C. Observation', '']
    successful = [r for r in document['runs'] if r['correctness'] == 'PASS']
    for run in successful:
        q = run['queues']['1']
        samples = [s for s in run['samples'] if s['consumer_connection'] == 1 and not s['producer_finished']]
        first, last = samples[0], samples[-1]
        lines.append(f"- {run['frames_target']} Frames: active SLOW samples changed from {first['pending_bytes']} to {last['pending_bytes']} bytes. At EOF: {q['at_eof_pending_frames']} Frames / {q['at_eof_pending_bytes']} bytes. Sample peak: {q['sample_peak_bytes']} bytes at {q['sample_peak_elapsed_ms']} ms; final zero at {q['final_zero_elapsed_ms']} ms, {q['drain_after_eof_s']:.3f} s after EOF. FAST sampled peak: {run['queues']['2']['sample_peak_bytes']} bytes.")
    if len(successful) == 3:
        peaks = [r['queues']['1']['exact_peak_bytes'] for r in successful]
        increasing = all(a < b for a, b in zip(peaks, peaks[1:]))
        lines.append(f"\nSLOW exact peak bytes, 300 → 900 → 1800: {peaks}. Strictly increasing: {increasing}. This observation is not a pass threshold.")
        lines += ['', '## D. Interpretation', '']
        if increasing and all(r['queues']['1']['at_eof_pending_bytes'] > 0 for r in successful):
            lines.append('The samples and peaks support that event-driven processing removed per-connection thread blocking, but an unbounded queue converts sustained consumer rate mismatch into application-level backlog growth. Producer nominal input is 30 Frames/s versus roughly 10 Frames/s SLOW pacing; kernel buffers and variable Frame sizes prevent assuming exactly 20 queued Frames/s. FAST sampled queues remained at the separately reported levels. All queues reached zero after Producer EOF.')
        else:
            lines.append('The measurements do not show a strictly increasing peak across all three lengths. Interpret the recorded curves as observed; no expected growth threshold was imposed. Inspect samples and logs for active and drain behavior.')
    lines += ['', '## E. Day 7 transition', '',
              'Queues remain unbounded. Sustained Slow Consumers can create backlog/memory growth risk. Day 7 will consider a queue bound and overload policy; neither is implemented here.', '',
              f"Correctness: {len(successful)}/{len(document['runs'])} runs passed. Metadata and complete summaries are in raw_runs.json; samples/ contains both Consumer curves, logs/ contains process stdout and timestamped timelines."]
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with (out / 'summary.csv').open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    (out / 'summary.md').write_text('\n'.join(lines) + '\n')
    (out / 'raw_runs.json').write_text(json.dumps(document, indent=2) + '\n')


async def main(args):
    if platform.system() != 'Linux':
        raise ValueError('Linux is required')
    tree, out = args.event_tree.resolve(), args.out.resolve()
    if any(out.iterdir()) if out.exists() else False:
        raise ValueError('Output directory must be absent or empty; existing results will not be overwritten')
    cache = (tree / BUILD / 'CMakeCache.txt').read_text()
    if not re.search(r'^CMAKE_BUILD_TYPE:STRING=Debug$', cache, re.M):
        raise ValueError('Day 6 Debug build required')
    metadata = {
        'commit': common.command_output(['git', 'rev-parse', 'HEAD'], tree),
        'tracked_status': common.tracked_status(tree),
        'tracked_diff': common.command_output(['git', 'diff', '--', 'src/server/main.cpp'], tree),
        'platform': platform.platform(), 'build_type': 'Debug',
        'compiler': common.command_output(['g++', '--version']).splitlines()[0],
        'cmake': common.command_output(['cmake', '--version']).splitlines()[0],
        'frames': LENGTHS, 'fps': 30, 'delay_ms': 100, 'timeouts_s': TIMEOUTS,
        'source_hashes': {p: hashlib.sha256((tree / p).read_bytes()).hexdigest() for p in (
            'src/server/main.cpp', 'src/producer/main.cpp', 'src/consumer/main.cpp',
            'include/eventstream/workload.hpp', 'include/eventstream/protocol.hpp')},
        'runner_sha256': hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
    }
    definitions = common.workload(tree)
    metadata['workload'] = definitions
    out.mkdir(parents=True, exist_ok=True)
    (out / 'samples').mkdir()
    document = {'metadata': metadata, 'runs': []}
    ports_used = set()
    for frames in LENGTHS:
        run = await run_one(tree, out, frames, definitions, ports_used)
        document['runs'].append(run)
        write_results(out, document)
        if run['correctness'] != 'PASS':
            raise RuntimeError('Correctness/orchestration failure; stopping')
    print(f'All three runs passed. Results: {out}', flush=True)


if __name__ == '__main__':
    asyncio.run(main(parse_args()))
