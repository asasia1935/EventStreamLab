#include <eventstream/protocol.hpp>

namespace {

void writeUint16BE(eventstream::WireHeader& wire, std::size_t offset,
                   std::uint16_t value) {
    wire[offset] = static_cast<std::uint8_t>((value >> 8) & 0xFF);
    wire[offset + 1] = static_cast<std::uint8_t>(value & 0xFF);
}

void writeUint32BE(eventstream::WireHeader& wire, std::size_t offset,
                   std::uint32_t value) {
    for (std::size_t i = 0; i < 4; ++i) {
        wire[offset + i] = static_cast<std::uint8_t>(
            (value >> (8 * (3 - i))) & 0xFF);
    }
}

void writeUint64BE(eventstream::WireHeader& wire, std::size_t offset,
                   std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        wire[offset + i] = static_cast<std::uint8_t>(
            (value >> (8 * (7 - i))) & 0xFF);
    }
}

std::uint16_t readUint16BE(const eventstream::WireHeader& wire,
                           std::size_t offset) {
    return (static_cast<std::uint16_t>(wire[offset]) << 8) |
           static_cast<std::uint16_t>(wire[offset + 1]);
}

std::uint32_t readUint32BE(const eventstream::WireHeader& wire,
                           std::size_t offset) {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value = (value << 8) | wire[offset + i];
    }
    return value;
}

std::uint64_t readUint64BE(const eventstream::WireHeader& wire,
                           std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        value = (value << 8) | wire[offset + i];
    }
    return value;
}

} // namespace

namespace eventstream {

WireHeader serializeHeader(const FrameHeader& header) {
    WireHeader wire{};

    writeUint32BE(wire, 0, header.magic);
    writeUint16BE(wire, 4, header.version);
    writeUint32BE(wire, 6, header.stream_id);
    writeUint64BE(wire, 10, header.sequence);
    writeUint64BE(wire, 18, header.timestamp_ns);
    writeUint32BE(wire, 26, header.payload_size);
    writeUint32BE(wire, 30, header.flags);

    return wire;
}

FrameHeader deserializeHeader(const WireHeader& wire) {
    return FrameHeader{
        readUint32BE(wire, 0),
        readUint16BE(wire, 4),
        readUint32BE(wire, 6),
        readUint64BE(wire, 10),
        readUint64BE(wire, 18),
        readUint32BE(wire, 26),
        readUint32BE(wire, 30),
    };
}

HeaderValidationError validateHeader(const FrameHeader& header) {
    if (header.magic != FRAME_MAGIC) {
        return HeaderValidationError::InvalidMagic;
    }
    if (header.version != PROTOCOL_VERSION) {
        return HeaderValidationError::UnsupportedVersion;
    }
    if (header.payload_size > MAX_PAYLOAD_SIZE) {
        return HeaderValidationError::PayloadTooLarge;
    }
    return HeaderValidationError::None;
}

} // namespace eventstream
