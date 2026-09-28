#include <eventstream/workload.hpp>

namespace eventstream {

std::vector<std::uint8_t> makeSyntheticPayloadBuffer() {
    std::vector<std::uint8_t> payload(LARGE_PAYLOAD_SIZE);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::uint8_t>(i & 0xFF);
    }
    return payload;
}

std::size_t payloadSizeForSequence(std::uint64_t sequence) {
    if (sequence != 0 && sequence % LARGE_FRAME_INTERVAL == 0) {
        return LARGE_PAYLOAD_SIZE;
    }
    return NORMAL_PAYLOAD_SIZE;
}

} // namespace eventstream
