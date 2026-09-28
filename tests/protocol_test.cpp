#include <eventstream/protocol.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>

namespace {

using eventstream::FrameHeader;
using eventstream::WireHeader;

constexpr std::array<std::uint8_t, eventstream::WIRE_HEADER_SIZE>
    KNOWN_WIRE_BYTES{
        0x45, 0x53, 0x4C, 0x46,
        0x00, 0x01,
        0x01, 0x02, 0x03, 0x04,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x00, 0x01, 0x02, 0x03,
        0xA1, 0xB2, 0xC3, 0xD4,
    };

bool checkHeaderFields(const FrameHeader& actual, const FrameHeader& expected,
                       const char* testName) {
    bool passed = true;

#define CHECK_FIELD(field)                                                     \
    do {                                                                       \
        if (actual.field != expected.field) {                                  \
            std::cerr << testName << " failed: " #field " mismatch\n";        \
            passed = false;                                                    \
        }                                                                      \
    } while (false)

    CHECK_FIELD(magic);
    CHECK_FIELD(version);
    CHECK_FIELD(stream_id);
    CHECK_FIELD(sequence);
    CHECK_FIELD(timestamp_ns);
    CHECK_FIELD(payload_size);
    CHECK_FIELD(flags);

#undef CHECK_FIELD
    return passed;
}

bool testSerializeKnownBytes() {
    const FrameHeader header{
        eventstream::FRAME_MAGIC,
        eventstream::PROTOCOL_VERSION,
        0x01020304,
        0x0102030405060708,
        0x1112131415161718,
        0x00010203,
        0xA1B2C3D4,
    };

    const WireHeader actual = eventstream::serializeHeader(header);
    static_assert(eventstream::WIRE_HEADER_SIZE == 34);
    static_assert(sizeof(WireHeader) == 34);

    for (std::size_t i = 0; i < KNOWN_WIRE_BYTES.size(); ++i) {
        if (actual[i] != KNOWN_WIRE_BYTES[i]) {
            std::cerr << "Serialization Known Bytes Test failed at byte " << i
                      << ": expected 0x" << std::hex
                      << static_cast<unsigned>(KNOWN_WIRE_BYTES[i])
                      << ", got 0x" << static_cast<unsigned>(actual[i])
                      << '\n';
            return false;
        }
    }

    std::cout << "Serialization Known Bytes Test passed\n";
    return true;
}

bool testDeserializeKnownBytes() {
    const WireHeader wire = KNOWN_WIRE_BYTES;
    const FrameHeader actual = eventstream::deserializeHeader(wire);
    const FrameHeader expected{
        eventstream::FRAME_MAGIC,
        eventstream::PROTOCOL_VERSION,
        0x01020304,
        0x0102030405060708,
        0x1112131415161718,
        0x00010203,
        0xA1B2C3D4,
    };

    if (!checkHeaderFields(actual, expected,
                           "Deserialization Known Bytes Test")) {
        return false;
    }

    std::cout << "Deserialization Known Bytes Test passed\n";
    return true;
}

bool testRoundTrip() {
    const FrameHeader original{
        eventstream::FRAME_MAGIC,
        eventstream::PROTOCOL_VERSION,
        0x0A0B0C0D,
        0x0102030405060708,
        0x2122232425262728,
        0x000ABCDE,
        0x55AA55AA,
    };

    const WireHeader wire = eventstream::serializeHeader(original);
    const FrameHeader restored = eventstream::deserializeHeader(wire);

    if (!checkHeaderFields(restored, original, "Round Trip Test")) {
        return false;
    }

    std::cout << "Round Trip Test passed\n";
    return true;
}

bool expectValidation(const char* caseName, const FrameHeader& header,
                      eventstream::HeaderValidationError expected) {
    const auto actual = eventstream::validateHeader(header);
    if (actual != expected) {
        std::cerr << caseName << " failed: expected HeaderValidationError "
                  << static_cast<int>(expected) << ", got "
                  << static_cast<int>(actual) << '\n';
        return false;
    }
    return true;
}

bool testValidation() {
    using eventstream::HeaderValidationError;

    const FrameHeader validHeader{
        eventstream::FRAME_MAGIC,
        eventstream::PROTOCOL_VERSION,
        42,
        123,
        456,
        1024,
        0,
    };

    if (!expectValidation("Valid Header", validHeader,
                          HeaderValidationError::None)) {
        return false;
    }

    auto invalidMagic = validHeader;
    invalidMagic.magic = eventstream::FRAME_MAGIC ^ 1;
    if (!expectValidation("Invalid Magic", invalidMagic,
                          HeaderValidationError::InvalidMagic)) {
        return false;
    }

    auto unsupportedVersion = validHeader;
    unsupportedVersion.version = eventstream::PROTOCOL_VERSION + 1;
    if (!expectValidation("Unsupported Version", unsupportedVersion,
                          HeaderValidationError::UnsupportedVersion)) {
        return false;
    }

    auto maxPayload = validHeader;
    maxPayload.payload_size = eventstream::MAX_PAYLOAD_SIZE;
    if (!expectValidation("Maximum Payload Boundary", maxPayload,
                          HeaderValidationError::None)) {
        return false;
    }

    auto oversizedPayload = validHeader;
    oversizedPayload.payload_size = eventstream::MAX_PAYLOAD_SIZE + 1;
    if (!expectValidation("Payload Above Maximum", oversizedPayload,
                          HeaderValidationError::PayloadTooLarge)) {
        return false;
    }

    std::cout << "Validation Tests passed\n";
    return true;
}

} // namespace

int main() {
    if (!testSerializeKnownBytes() || !testDeserializeKnownBytes() ||
        !testRoundTrip() || !testValidation()) {
        return 1;
    }

    std::cout << "All protocol tests passed\n";
    return 0;
}
