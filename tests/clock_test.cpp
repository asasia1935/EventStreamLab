#include <eventstream/clock.hpp>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

int main() {
    const std::uint64_t first = eventstream::monotonicNowNs();
    if (first == 0) {
        std::cerr << "monotonicNowNs returned 0\n";
        return 1;
    }

    std::uint64_t previous = first;
    for (int i = 0; i < 100; ++i) {
        const std::uint64_t current = eventstream::monotonicNowNs();
        if (current == 0) {
            std::cerr << "monotonicNowNs returned 0 during monotonic check\n";
            return 1;
        }
        if (current < previous) {
            std::cerr << "monotonicNowNs decreased\n";
            return 1;
        }
        previous = current;
    }

    const std::uint64_t start = eventstream::monotonicNowNs();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    const std::uint64_t end = eventstream::monotonicNowNs();

    if (start == 0 || end == 0) {
        std::cerr << "monotonicNowNs returned 0 around sleep\n";
        return 1;
    }
    if (end <= start) {
        std::cerr << "monotonicNowNs did not advance after sleep\n";
        return 1;
    }

    std::cout << "Clock tests passed\n";
    return 0;
}
