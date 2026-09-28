#include <eventstream/protocol.hpp>
#include <eventstream/workload.hpp>

#include <cstddef>
#include <cstdint>
#include <iostream>

namespace {

bool testSyntheticPayloadSize() {
    const auto payload = eventstream::makeSyntheticPayloadBuffer();
    if (payload.size() != eventstream::LARGE_PAYLOAD_SIZE) {
        std::cerr << "Synthetic Payload Size Test failed: expected "
                  << eventstream::LARGE_PAYLOAD_SIZE << ", got "
                  << payload.size() << '\n';
        return false;
    }

    std::cout << "Synthetic Payload Size Test passed\n";
    return true;
}

bool testSyntheticPayloadPattern() {
    const auto payload = eventstream::makeSyntheticPayloadBuffer();
    for (std::size_t i = 0; i < payload.size(); ++i) {
        const auto expected = static_cast<std::uint8_t>(i & 0xFF);
        if (payload[i] != expected) {
            std::cerr << "Synthetic Payload Pattern Test failed at index " << i
                      << ": expected " << static_cast<unsigned>(expected)
                      << ", got " << static_cast<unsigned>(payload[i]) << '\n';
            return false;
        }
    }

    std::cout << "Synthetic Payload Pattern Test passed\n";
    return true;
}

bool testPayloadSizeRule() {
    using eventstream::LARGE_PAYLOAD_SIZE;
    using eventstream::NORMAL_PAYLOAD_SIZE;
    using eventstream::payloadSizeForSequence;

    constexpr struct {
        std::uint64_t sequence;
        std::size_t expectedSize;
    } cases[]{
        {0, NORMAL_PAYLOAD_SIZE},
        {1, NORMAL_PAYLOAD_SIZE},
        {29, NORMAL_PAYLOAD_SIZE},
        {30, LARGE_PAYLOAD_SIZE},
        {31, NORMAL_PAYLOAD_SIZE},
        {59, NORMAL_PAYLOAD_SIZE},
        {60, LARGE_PAYLOAD_SIZE},
        {90, LARGE_PAYLOAD_SIZE},
    };

    for (const auto& testCase : cases) {
        const auto actual = payloadSizeForSequence(testCase.sequence);
        if (actual != testCase.expectedSize) {
            std::cerr << "Payload Size Rule Test failed for sequence "
                      << testCase.sequence << ": expected "
                      << testCase.expectedSize << ", got " << actual << '\n';
            return false;
        }
    }

    std::cout << "Payload Size Rule Test passed\n";
    return true;
}

bool testProtocolCompatibility() {
    static_assert(eventstream::LARGE_PAYLOAD_SIZE <=
                  eventstream::MAX_PAYLOAD_SIZE);
    std::cout << "Workload Protocol Compatibility Test passed\n";
    return true;
}

} // namespace

int main() {
    if (!testSyntheticPayloadSize() || !testSyntheticPayloadPattern() ||
        !testPayloadSizeRule() || !testProtocolCompatibility()) {
        return 1;
    }

    std::cout << "All workload tests passed\n";
    return 0;
}
