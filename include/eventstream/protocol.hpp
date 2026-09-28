#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace eventstream {

constexpr std::uint32_t FRAME_MAGIC = 0x45534C46;
constexpr std::uint16_t PROTOCOL_VERSION = 1;
constexpr std::uint32_t MAX_PAYLOAD_SIZE = 1024 * 1024;

// Wire offsets: magic 0, version 4, stream_id 6, sequence 10,
// timestamp_ns 18, payload_size 26, flags 30. Integer representation will be
// Big Endian; this contract is independent of FrameHeader's memory layout.
constexpr std::size_t WIRE_HEADER_SIZE = 34;

struct FrameHeader {
    std::uint32_t magic;
    std::uint16_t version;
    std::uint32_t stream_id;
    std::uint64_t sequence;
    std::uint64_t timestamp_ns;
    std::uint32_t payload_size;
    std::uint32_t flags;
};

enum class HeaderValidationError {
    None,
    InvalidMagic,
    UnsupportedVersion,
    PayloadTooLarge,
};

using WireHeader = std::array<std::uint8_t, WIRE_HEADER_SIZE>;

WireHeader serializeHeader(const FrameHeader& header);
FrameHeader deserializeHeader(const WireHeader& wire);
HeaderValidationError validateHeader(const FrameHeader& header);

} // namespace eventstream
