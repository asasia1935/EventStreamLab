"""Plot committed EventStreamLab measurements; never launch a benchmark.

Run from any directory. PNGs use raw samples and Day 5 completion medians;
printed CSV-derived values support the bilingual README consistency check.
"""

import argparse
import csv
import json
import os
from pathlib import Path
import statistics
import tempfile

os.environ.setdefault('MPLCONFIGDIR', str(Path(tempfile.gettempdir()) / 'eventstreamlab-matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'benchmarks' / 'results'
MIB = 1024 * 1024
COLORS = ('#496c8a', '#d77b38')


def rows(relative):
    with (RESULTS / relative).open(newline='', encoding='utf-8') as source:
        return list(csv.DictReader(source))


def median(group, key):
    return statistics.median(float(row[key]) for row in group)


def slow_samples(relative):
    samples = [r for r in rows(relative) if r['consumer_connection'] == '1']
    return samples, [int(r['elapsed_ms']) / 1000 for r in samples], [int(r['pending_bytes']) / MIB for r in samples]


def source_values():
    day5 = rows('day5-debug/summary.csv')
    day6 = rows('day6-debug/summary.csv')
    day7 = rows('day7-debug/summary.csv')
    day8 = rows('day8-release/summary.csv')
    events = rows('day8-release/events.csv')
    return {
        'day5_medians_s': {
            architecture: {label: median([r for r in day5 if r['architecture'] == architecture and r['scenario'] == 'SLOW/FAST'], key)
                           for label, key in (('Producer', 'producer_completion_s'), ('FAST', 'fast_completion_s'), ('SLOW', 'slow_completion_s'), ('Server', 'server_completion_s'))}
            for architecture in ('Blocking', 'Event-Driven')},
        'day6': [{**r, 'slow_exact_peak_mib': int(r['slow_exact_peak_bytes']) / MIB} for r in day6],
        'day7': day7,
        'day8_medians_s': {
            scenario: {key: median([r for r in day8 if r['scenario'] == scenario], key)
                       for key in ('producer_completion_s', 'consumer1_completion_s', 'consumer2_completion_s', 'server_completion_s')}
            for scenario in ('FAST/FAST', 'SLOW/FAST overload')},
        'day8_under_limit': next(r for r in day8 if r['scenario'] == 'SLOW/FAST under-limit'),
        'day8_max_slow_peak_bytes': max(int(r['consumer1_max_pending_bytes']) for r in day8 if r['scenario'] == 'SLOW/FAST overload'),
        'day8_overflow_events': events,
        'queue_limit_bytes': int(events[0]['limit_bytes']),
        'day8_passed_runs': sum(r['correctness'] == 'PASS' for r in day8),
    }


def finish(fig, ax, target):
    ax.set_axisbelow(True)
    ax.grid(axis='y', alpha=0.25)
    fig.tight_layout()
    fig.savefig(target, dpi=180, facecolor='white', metadata={'Software': 'EventStreamLab plot_results.py'})
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'docs' / 'images')
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    values = source_values()
    print(json.dumps(values, indent=2))
    with plt.rc_context({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False}):
        fig, ax = plt.subplots(figsize=(8, 4.5))
        labels = ('Producer', 'FAST Consumer', 'SLOW Consumer', 'Server')
        keys = ('Producer', 'FAST', 'SLOW', 'Server')
        for series, architecture in enumerate(('Blocking', 'Event-Driven')):
            heights = [values['day5_medians_s'][architecture][key] for key in keys]
            x = [i + (series - 0.5) * 0.36 for i in range(len(labels))]
            bars = ax.bar(x, heights, width=0.36, color=COLORS[series], label=architecture,
                          hatch='//' if series == 0 else '', edgecolor='white')
            ax.bar_label(bars, labels=[f'{v:.2f}' for v in heights], padding=3, fontsize=9)
        ax.set_xticks(range(len(labels)), labels)
        ax.set_ylim(0, max(ax.get_ylim()[1], max(heights) * 1.18))
        ax.set_ylabel('Completion time (seconds)')
        ax.set_title('Day 5: Blocking vs Event-Driven\nLinux Debug, SLOW/FAST, median of 3 runs')
        ax.legend(loc='upper left')
        finish(fig, ax, args.output_dir / 'blocking-vs-event.png')

        samples, x, y = slow_samples('day6-debug/samples/frames-1800.csv')
        eof = next(int(r['elapsed_ms']) / 1000 for r in samples if r['producer_finished'] == '1')
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(x, y, color=COLORS[0], label='SLOW #1 raw queue samples')
        ax.axvline(eof, color='#555555', linestyle='--', label=f'Producer EOF ({eof:.2f} s)')
        ax.set_xlabel('Server event-loop elapsed time (seconds)')
        ax.set_ylabel('Logical pending TX backlog (MiB)')
        ax.set_title('Day 6: Unbounded Queue Growth and Drain\nLinux Debug, 1800 Frames, ~1 second samples')
        ax.legend()
        finish(fig, ax, args.output_dir / 'queue-growth.png')

        _, bounded_x, bounded_y = slow_samples('day8-release/samples/slow-fast-overload-1800-run1.csv')
        event = next(e for e in values['day8_overflow_events'] if e['scenario'] == 'SLOW/FAST overload' and e['run'] == '1' and e['consumer_connection'] == '1')
        event_x, event_y = int(event['elapsed_ms']) / 1000, int(event['pending_bytes']) / MIB
        limit_mib = int(event['limit_bytes']) / MIB
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(x, y, color=COLORS[0], label='Day 6 Debug: unbounded')
        ax.plot(bounded_x, bounded_y, color=COLORS[1], linestyle='--', label='Day 8 Release: bounded (run 1)')
        ax.axhline(limit_mib, color='#555555', linestyle=':', label=f'{limit_mib:g} MiB logical backlog limit')
        ax.scatter([event_x], [event_y], color=COLORS[1], marker='X', s=65, zorder=5)
        ax.annotate(f'Overflow: disconnect SLOW\n{event_x:.3f} s', xy=(event_x, event_y),
                    xytext=(event_x + 15, event_y + 18), arrowprops={'arrowstyle': '->'}, fontsize=9)
        ax.set_xlabel('Elapsed seconds from each Server event-loop start')
        ax.set_ylabel('Logical pending TX backlog (MiB)')
        ax.set_title('Unbounded vs Bounded Overload\nQueue behavior comparison; independent runs and build types')
        ax.legend(loc='upper right')
        finish(fig, ax, args.output_dir / 'bounded-overload.png')
    print(f'Wrote 3 PNGs to {args.output_dir}')


if __name__ == '__main__':
    main()
