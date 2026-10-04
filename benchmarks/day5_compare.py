"""Linux Debug architectural benchmark for Blocking versus Event-Driven Server.

Launch independent OS processes, measure wall-clock completion, validate
correctness, and write raw and summarized results.
"""

import argparse
import ast
import asyncio
import csv
import hashlib
import json
import pathlib
import platform
import re
import socket
import statistics
import subprocess
import time

FRAMES = 900
FPS = 30
DELAY_MS = 100
BUILD_DIRECTORY = pathlib.Path('out/day5-debug')
QUEUE_KEYS = ('eagain_count', 'epollout_wakes', 'max_pending_frames', 'max_pending_bytes')
CSV_COLUMNS = (
    'architecture', 'scenario', 'run', 'producer_completion_s',
    'consumer1_completion_s', 'consumer2_completion_s', 'slow_completion_s',
    'fast_completion_s', 'server_completion_s',
    *(f'slow_{key}' for key in QUEUE_KEYS),
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event-tree', type=pathlib.Path, default=pathlib.Path('.'))
    parser.add_argument('--blocking-tree', type=pathlib.Path, required=True)
    parser.add_argument('--out', type=pathlib.Path, default=pathlib.Path('/tmp/eventstreamlab-day5'))
    return parser.parse_args(argv)


def command_output(command, cwd=None):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def tracked_status(tree):
    return command_output(['git', 'status', '--porcelain', '--untracked-files=no'], tree)


def source_constant(tree, relative_path, name):
    text = (tree / relative_path).read_text()
    match = re.search(r'\b' + re.escape(name) + r'\s*=\s*([^;]+);', text)
    if match is None:
        raise ValueError(f'Missing source constant {name}')

    def evaluate(node):
        if isinstance(node, ast.Constant) and type(node.value) is int:
            return node.value
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mult):
            return evaluate(node.left) * evaluate(node.right)
        raise ValueError(f'Unsupported expression for {name}')

    return evaluate(ast.parse(match.group(1), mode='eval').body)


def workload(tree):
    values = {
        key: source_constant(tree, 'include/eventstream/workload.hpp', name)
        for key, name in (
            ('normal_payload_bytes', 'NORMAL_PAYLOAD_SIZE'),
            ('large_payload_bytes', 'LARGE_PAYLOAD_SIZE'),
            ('large_interval', 'LARGE_FRAME_INTERVAL'),
        )
    }
    values['wire_header_bytes'] = source_constant(
        tree, 'include/eventstream/protocol.hpp', 'WIRE_HEADER_SIZE')
    large_count = FRAMES // values['large_interval']
    values['expected_wire_bytes_per_consumer'] = (
        (FRAMES - large_count) * values['normal_payload_bytes']
        + large_count * values['large_payload_bytes']
        + FRAMES * values['wire_header_bytes'])
    return values


def collect_metadata(trees):
    definitions = {name: workload(tree) for name, tree in trees.items()}
    if definitions['Blocking'] != definitions['Event-Driven']:
        raise ValueError('Workload/protocol constants differ between trees')
    result = {
        'date_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'platform': platform.platform(),
        'kernel': command_output(['uname', '-a']),
        'compiler': command_output(['g++', '--version']).splitlines()[0],
        'cmake': command_output(['cmake', '--version']).splitlines()[0],
        'build_type': 'Debug', 'frames': FRAMES, 'fps': FPS, 'slow_delay_ms': DELAY_MS,
        'primary_clock': 'time.monotonic(): independent process completion minus timestamp immediately before Producer launch',
        'timeout_seconds': {'SLOW/FAST': 240, 'FAST/FAST': 90},
        'workload': definitions['Blocking'], 'trees': {}, 'source_hashes': {},
        'runner_provenance': 'Recreated after loss of the original Day 5 temporary artifacts',
        'send_time_note': 'Blocking sendAll time includes waits; Event active send time excludes EPOLLOUT waits. No direct speedup comparison of these counters.',
    }
    for name, tree in trees.items():
        dirty = tracked_status(tree)
        if dirty:
            raise ValueError(f'{name} has tracked modifications:\n{dirty}')
        cache = (tree / BUILD_DIRECTORY / 'CMakeCache.txt').read_text()
        if not re.search(r'^CMAKE_BUILD_TYPE:STRING=Debug$', cache, re.M):
            raise ValueError(f'{name} build must be Debug')
        compiler = re.search(r'^CMAKE_CXX_COMPILER:FILEPATH=(.+)$', cache, re.M)
        if compiler is None:
            raise ValueError(f'{name} compiler path is missing from CMakeCache.txt')
        result['trees'][name] = {
            'source': str(tree), 'build': str(tree / BUILD_DIRECTORY),
            'commit': command_output(['git', 'rev-parse', 'HEAD'], tree),
            'tracked_status_before': dirty, 'compiler_path': compiler.group(1),
        }
    if len({data['compiler_path'] for data in result['trees'].values()}) != 1:
        raise ValueError('Build compiler paths differ')
    tag = subprocess.run(
        ['git', 'rev-parse', '--verify', 'blocking-baseline-v0.1^{commit}'],
        cwd=trees['Blocking'], text=True, capture_output=True)
    result['baseline_tag_commit'] = tag.stdout.strip() if tag.returncode == 0 else None
    if tag.returncode == 0 and tag.stdout.strip() != result['trees']['Blocking']['commit']:
        raise ValueError('Blocking HEAD does not match blocking-baseline-v0.1')
    for relative in ('src/producer/main.cpp', 'src/consumer/main.cpp',
                     'src/common/workload.cpp', 'src/common/protocol.cpp'):
        hashes = {
            name: hashlib.sha256((tree / relative).read_bytes()).hexdigest()
            for name, tree in trees.items()
        }
        if len(set(hashes.values())) != 1:
            raise ValueError(f'Source differs between trees: {relative}')
        result['source_hashes'][relative] = hashes
    return result


class Process:
    def __init__(self, role, child, command, log_path):
        self.role, self.child, self.command = role, child, command
        self.log_path = log_path
        self.lines = []
        self.notifications = asyncio.Queue()
        self.completion = None
        self.reader = asyncio.create_task(self.capture())
        self.watcher = asyncio.create_task(self.watch())

    async def capture(self):
        with self.log_path.open('w') as log, self.log_path.with_suffix('.jsonl').open('w') as timeline:
            while data := await self.child.stdout.readline():
                line = data.decode('utf-8', errors='replace').rstrip('\n')
                self.lines.append(line)
                log.write(line + '\n')
                log.flush()
                timeline.write(json.dumps({'monotonic_s': time.monotonic(), 'line': line}) + '\n')
                timeline.flush()
                self.notifications.put_nowait(line)
        self.notifications.put_nowait(None)

    async def watch(self):
        await self.child.wait()
        self.completion = time.monotonic()

    async def wait_line(self, text):
        async def wait():
            while True:
                line = await self.notifications.get()
                if line is None:
                    raise RuntimeError(f'{self.role} exited before: {text}')
                if text in line:
                    return
        await asyncio.wait_for(wait(), timeout=10)


def choose_ports(used_ports):
    reservations = []
    try:
        while len(reservations) < 2:
            reservation = socket.socket()
            try:
                reservation.bind(('127.0.0.1', 0))
                port = reservation.getsockname()[1]
                if port in used_ports:
                    reservation.close()
                    continue
                used_ports.add(port)
                reservations.append(reservation)
            except BaseException:
                reservation.close()
                raise
        return [sock.getsockname()[1] for sock in reservations]
    finally:
        for sock in reservations:
            sock.close()


def parse_values(lines):
    values = {}
    for line in lines:
        match = re.fullmatch(r'([a-z0-9_]+)=(.*)', line)
        if match:
            key, value = match.groups()
            values[key] = int(value) if value.isdigit() else value
    return values


def parse_server(lines):
    groups, top, current = {}, [], None
    for line in lines[lines.index('Server summary') + 1:]:
        if line.startswith('consumer_connection='):
            current = line.split('=')[1]
            groups[current] = []
        elif current is None:
            top.append(line)
        else:
            groups[current].append(line)
    return {'summary': parse_values(top),
            'connections': {key: parse_values(value) for key, value in groups.items()}}


def validate_run(record, processes, expected_bytes):
    for role, proc in processes.items():
        if proc.child.returncode != 0:
            raise ValueError(f'{role} exit code {proc.child.returncode}')
        if any(re.search(r'failed|mismatch|rejected|incomplete|unexpected|error', line, re.I)
               for line in proc.lines):
            raise ValueError(f'{role} reported an error; see its log')
    if record['producer']['frames_sent'] != FRAMES:
        raise ValueError('Producer frame count mismatch')
    if record['server']['summary']['frames_received'] != FRAMES:
        raise ValueError('Server frame count mismatch')
    for index in ('1', '2'):
        consumer = record['consumers'][index]
        connection = record['server']['connections'][index]
        if consumer['frames_received'] != FRAMES or connection['frames_forwarded'] != FRAMES:
            raise ValueError(f'Consumer {index} frame count mismatch')
        expected_mode = 'slow' if index == '1' and record['scenario'] == 'SLOW/FAST' else 'fast'
        if consumer['mode'] != expected_mode or consumer['id'] != (10 if index == '1' else 1):
            raise ValueError(f'Consumer {index} role mismatch')
        if record['architecture'] == 'Event-Driven':
            if (connection['frames_enqueued'] != FRAMES
                    or connection['frames_completed'] != FRAMES
                    or connection['bytes_sent'] != expected_bytes):
                raise ValueError(f'Consumer {index} TX counters mismatch')


async def run_one(trees, out, architecture, scenario, number, expected_bytes, used_ports):
    name = f"{architecture.lower().replace('-', '')}_{scenario.lower().replace('/', '_')}_{number}"
    log_dir = out / 'logs' / name
    log_dir.mkdir(parents=True)
    processes = {}
    record = {'architecture': architecture, 'scenario': scenario, 'run': number, 'logs': str(log_dir)}

    async def launch(role, executable, arguments):
        command = ['stdbuf', '-oL', '-eL', str(trees[architecture] / BUILD_DIRECTORY / executable), *arguments]
        child = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        proc = Process(role, child, command, log_dir / f'{role}.log')
        processes[role] = proc
        return proc

    try:
        producer_port, consumer_port = choose_ports(used_ports)
        record.update(producer_port=producer_port, consumer_port=consumer_port)
        print(f'START {name}', flush=True)
        server = await launch('server', 'esl_server', [
            '--producer-port', str(producer_port), '--consumer-port', str(consumer_port),
            '--expected-consumers', '2'])
        await server.wait_line('Producer connection waiting...')
        mode1 = 'slow' if scenario == 'SLOW/FAST' else 'fast'
        arguments = ['--host', '127.0.0.1', '--port', str(consumer_port), '--mode', mode1, '--id', '10']
        if mode1 == 'slow':
            arguments += ['--delay-ms', str(DELAY_MS)]
        consumer1 = await launch('consumer1', 'esl_consumer', arguments)
        await consumer1.wait_line('Connected to server')
        consumer2 = await launch('consumer2', 'esl_consumer', [
            '--host', '127.0.0.1', '--port', str(consumer_port), '--mode', 'fast', '--id', '1'])
        await consumer2.wait_line('Connected to server')
        t0 = time.monotonic()
        record['producer_launch_t0_monotonic_s'] = t0
        await launch('producer', 'esl_producer', [
            '--host', '127.0.0.1', '--port', str(producer_port),
            '--fps', str(FPS), '--frames', str(FRAMES)])
        timeout = 240 if scenario == 'SLOW/FAST' else 90
        monitors = asyncio.gather(*(proc.watcher for proc in processes.values()))
        await asyncio.wait_for(asyncio.shield(monitors), timeout=timeout)
        await asyncio.gather(*(proc.reader for proc in processes.values()))
        record['primary'] = {f'{role}_completion_s': proc.completion - t0
                             for role, proc in processes.items()}
        if scenario == 'SLOW/FAST':
            record['primary']['slow_completion_s'] = record['primary']['consumer1_completion_s']
            record['primary']['fast_completion_s'] = record['primary']['consumer2_completion_s']
        producer_lines = processes['producer'].lines
        record['producer'] = parse_values(producer_lines[producer_lines.index('Producer summary') + 1:])
        record['consumers'] = {str(i): parse_values(processes[f'consumer{i}'].lines) for i in (1, 2)}
        record['server'] = parse_server(server.lines)
        validate_run(record, processes, expected_bytes)
        record['correctness'] = 'PASS'
        print(f"DONE {name} {json.dumps(record['primary'])}", flush=True)
    except Exception as failure:
        record['correctness'] = 'FAIL'
        record['error'] = repr(failure)
        print(f'FAIL {name}: {failure}', flush=True)
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
                proc.child.kill()
                await proc.child.wait()
        await asyncio.gather(*(proc.reader for proc in processes.values()), return_exceptions=True)
        await asyncio.gather(*(proc.watcher for proc in processes.values()), return_exceptions=True)
        record['commands'] = {role: proc.command for role, proc in processes.items()}
        record['exit_codes'] = {role: proc.child.returncode for role, proc in processes.items()}
    return record


def stats(values):
    return {'median': statistics.median(values), 'min': min(values), 'max': max(values)}


def write_results(out, document):
    runs = document['runs']
    successful = [run for run in runs if run['correctness'] == 'PASS']
    with (out / 'summary.csv').open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for run in runs:
            row = {key: run[key] for key in ('architecture', 'scenario', 'run')}
            row.update(run.get('primary', {}))
            if run in successful and run['architecture'] == 'Event-Driven' and run['scenario'] == 'SLOW/FAST':
                row.update({f'slow_{key}': run['server']['connections']['1'][key] for key in QUEUE_KEYS})
            writer.writerow(row)
    lines = ['# EventStreamLab Day 5 — Linux Debug architectural comparison', '',
             '30 FPS, 900 Frames, SLOW #1 delay 100 ms, FAST #2. Primary seconds are measured from immediately before Producer launch to independent process completion.', '',
             '## A. SLOW/FAST raw runs', '',
             '| Architecture | Run | Producer | FAST | SLOW | Server |',
             '| --- | ---: | ---: | ---: | ---: | ---: |']
    for run in successful:
        if run['scenario'] == 'SLOW/FAST':
            p = run['primary']
            lines.append(f"| {run['architecture']} | {run['run']} | {p['producer_completion_s']:.3f} | {p['fast_completion_s']:.3f} | {p['slow_completion_s']:.3f} | {p['server_completion_s']:.3f} |")
    if len(successful) == 8:
        document['statistics'] = {
            metric: {architecture: stats([run['primary'][metric] for run in successful
                                         if run['architecture'] == architecture and run['scenario'] == 'SLOW/FAST'])
                     for architecture in ('Blocking', 'Event-Driven')}
            for metric in ('producer_completion_s', 'fast_completion_s', 'slow_completion_s', 'server_completion_s')
        }
        lines += ['', '## B. Median comparison', '',
                  'Difference = Event − Blocking. Ranges are min–max.', '',
                  '| Metric | Blocking median [range] | Event median [range] | Difference |',
                  '| --- | ---: | ---: | ---: |']
        for metric, groups in document['statistics'].items():
            b, e = groups['Blocking'], groups['Event-Driven']
            difference = e['median'] - b['median']
            lines.append(f"| {metric} | {b['median']:.3f} [{b['min']:.3f}–{b['max']:.3f}] | {e['median']:.3f} [{e['min']:.3f}–{e['max']:.3f}] | {difference:+.3f} s ({difference / b['median'] * 100:+.1f}%) |")
        queue_runs = [run['server']['connections']['1'] for run in successful
                      if run['architecture'] == 'Event-Driven' and run['scenario'] == 'SLOW/FAST']
        document['queue_statistics'] = {key: stats([run[key] for run in queue_runs]) for key in QUEUE_KEYS}
        lines += ['', '## C. Event Slow queue/readiness', '',
                  '| Run | EAGAIN | EPOLLOUT | Max pending frames | Max pending bytes |',
                  '| --- | ---: | ---: | ---: | ---: |']
        for number, metrics in enumerate(queue_runs, 1):
            lines.append('| ' + str(number) + ' | ' + ' | '.join(str(metrics[key]) for key in QUEUE_KEYS) + ' |')
        for statistic in ('median', 'min', 'max'):
            lines.append('| ' + statistic + ' | ' + ' | '.join(str(document['queue_statistics'][key][statistic]) for key in QUEUE_KEYS) + ' |')
    lines += ['', '## D. FAST/FAST control', '',
              '| Architecture | Producer | Consumer #1 | Consumer #2 | Server |',
              '| --- | ---: | ---: | ---: | ---: |']
    for run in successful:
        if run['scenario'] == 'FAST/FAST':
            p = run['primary']
            lines.append(f"| {run['architecture']} | {p['producer_completion_s']:.3f} | {p['consumer1_completion_s']:.3f} | {p['consumer2_completion_s']:.3f} | {p['server_completion_s']:.3f} |")
    lines += ['', f'Correctness: {len(successful)}/{len(runs)} completed runs passed.', '',
              'Blocking sendAll time includes waiting; Event active send time excludes EPOLLOUT wait. These counters are not used for direct speedup calculations. Server completion includes pending TX drain. Queue peaks are unsent wire bytes, not RSS.', '',
              'Environment, commit hashes, compiler paths and source hashes are recorded in raw_runs.json; per-process logs and timestamped stdout are in logs/.']
    (out / 'summary.md').write_text('\n'.join(lines) + '\n')
    (out / 'raw_runs.json').write_text(json.dumps(document, indent=2) + '\n')


async def main(args):
    if platform.system() != 'Linux':
        raise ValueError('This benchmark requires Linux')
    trees = {'Blocking': args.blocking_tree.resolve(), 'Event-Driven': args.event_tree.resolve()}
    out = args.out.resolve()
    if (out / 'raw_runs.json').exists() or (out / 'logs').exists():
        raise ValueError('Refusing to overwrite an existing benchmark output directory')
    document = {'metadata': collect_metadata(trees), 'runs': []}
    out.mkdir(parents=True, exist_ok=True)
    used_ports = set()
    plan = [(architecture, 'SLOW/FAST', number) for number in range(1, 4) for architecture in trees]
    plan += [(architecture, 'FAST/FAST', 1) for architecture in trees]
    for architecture, scenario, number in plan:
        record = await run_one(trees, out, architecture, scenario, number,
                               document['metadata']['workload']['expected_wire_bytes_per_consumer'], used_ports)
        document['runs'].append(record)
        write_results(out, document)
        if record['correctness'] != 'PASS':
            raise RuntimeError('Benchmark correctness failure; stopped')
    for name, tree in trees.items():
        state = tracked_status(tree)
        document['metadata']['trees'][name]['tracked_status_after'] = state
        if state:
            write_results(out, document)
            raise ValueError(f'{name} has tracked modifications after the benchmark')
    write_results(out, document)
    print(f'All 8 runs passed; results: {out}', flush=True)


if __name__ == '__main__':
    asyncio.run(main(parse_args()))
