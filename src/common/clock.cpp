#include <eventstream/clock.hpp>

#if defined(_WIN32)
#define NOMINMAX
#include <windows.h>
#elif defined(__linux__)
#include <time.h>
#else
#error "monotonicNowNs() is supported on Windows and Linux"
#endif

namespace eventstream {

std::uint64_t monotonicNowNs() {
#if defined(_WIN32)
    static const std::uint64_t frequency = [] {
        LARGE_INTEGER value{};
        return QueryPerformanceFrequency(&value)
                   ? static_cast<std::uint64_t>(value.QuadPart)
                   : std::uint64_t{0};
    }();

    LARGE_INTEGER counter{};
    if (frequency == 0 || !QueryPerformanceCounter(&counter)) {
        return 0;
    }

    const auto ticks = static_cast<std::uint64_t>(counter.QuadPart);
    const std::uint64_t seconds = ticks / frequency;
    const std::uint64_t remainder = ticks % frequency;
    constexpr std::uint64_t nanosecondsPerSecond = 1'000'000'000ULL;

    return seconds * nanosecondsPerSecond +
           (remainder * nanosecondsPerSecond) / frequency;
#elif defined(__linux__)
    timespec value{};
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        return 0;
    }

    constexpr std::uint64_t nanosecondsPerSecond = 1'000'000'000ULL;
    return static_cast<std::uint64_t>(value.tv_sec) * nanosecondsPerSecond +
           static_cast<std::uint64_t>(value.tv_nsec);
#endif
}

} // namespace eventstream
