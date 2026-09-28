#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace eventstream {

constexpr std::size_t NORMAL_PAYLOAD_SIZE = 100 * 1024;
constexpr std::size_t LARGE_PAYLOAD_SIZE = 500 * 1024;
constexpr std::uint64_t LARGE_FRAME_INTERVAL = 30;

std::vector<std::uint8_t> makeSyntheticPayloadBuffer();
std::size_t payloadSizeForSequence(std::uint64_t sequence);

} // namespace eventstream
