#include <eventstream/cli.hpp>
#include <eventstream/protocol.hpp>
#include <eventstream/socket.hpp>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

namespace {

const char* validationErrorMessage(
    eventstream::HeaderValidationError validationError) {
    using eventstream::HeaderValidationError;
    switch (validationError) {
    case HeaderValidationError::None:
        return "Header validation passed";
    case HeaderValidationError::InvalidMagic:
        return "Header rejected: invalid magic";
    case HeaderValidationError::UnsupportedVersion:
        return "Header rejected: unsupported protocol version";
    case HeaderValidationError::PayloadTooLarge:
        return "Header rejected: payload size exceeds the protocol maximum";
    }
    return "Header rejected: unknown validation error";
}

} // namespace

int main(int argc, char* argv[]) {
    eventstream::ConsumerOptions options;
    std::string error;
    if (!eventstream::parseConsumerOptions(argc, argv, options, error)) {
        std::cerr << error << '\n';
        return 1;
    }

    std::cout << "EventStreamLab Consumer\n";
    std::cout << "host=" << options.host << '\n'
              << "port=" << options.port << '\n'
              << "mode="
              << (options.mode == eventstream::ConsumerMode::Fast ? "fast"
                                                                  : "slow")
              << '\n'
              << "id=" << options.id << '\n'
              << "delay_ms=" << options.delay_ms << '\n';

    if (!eventstream::initializeSockets(error)) {
        std::cerr << error << '\n';
        return 1;
    }

    std::cout << "Connecting to " << options.host << ':' << options.port
              << "...\n";
    const eventstream::SocketHandle socket =
        eventstream::connectTcp(options.host, options.port, error);
    if (!eventstream::isValidSocket(socket)) {
        std::cerr << error << '\n';
        eventstream::cleanupSockets();
        return 1;
    }

    std::cout << "Connected to server\n";

    std::uint64_t expectedSequence = 1;
    std::uint64_t framesReceived = 0;
    const auto streamStart = std::chrono::steady_clock::now();
    auto streamEnd = streamStart;
    for (;;) {
        eventstream::WireHeader wireHeader{};
        const auto receiveResult = eventstream::recvExactWithStatus(
            socket, wireHeader.data(), wireHeader.size(), error);
        if (receiveResult == eventstream::RecvExactResult::PeerClosed) {
            std::cout << "Stream ended normally\n";
            streamEnd = std::chrono::steady_clock::now();
            break;
        }
        if (receiveResult == eventstream::RecvExactResult::Error) {
            std::cerr << "Failed to receive frame header: " << error << '\n';
            eventstream::closeSocket(socket);
            eventstream::cleanupSockets();
            return 1;
        }

        const eventstream::FrameHeader header =
            eventstream::deserializeHeader(wireHeader);
        const auto validation = eventstream::validateHeader(header);
        if (validation != eventstream::HeaderValidationError::None) {
            std::cerr << validationErrorMessage(validation) << '\n';
            eventstream::closeSocket(socket);
            eventstream::cleanupSockets();
            return 1;
        }
        if (header.sequence != expectedSequence) {
            std::cerr << "Frame sequence mismatch: expected "
                      << expectedSequence << ", got " << header.sequence
                      << '\n';
            eventstream::closeSocket(socket);
            eventstream::cleanupSockets();
            return 1;
        }

        std::vector<std::uint8_t> payload(header.payload_size);
        if (!eventstream::recvExact(socket, payload.data(), payload.size(),
                                    error)) {
            std::cerr << "Failed to receive frame payload: " << error << '\n';
            eventstream::closeSocket(socket);
            eventstream::cleanupSockets();
            return 1;
        }

        for (std::size_t i = 0; i < payload.size(); ++i) {
            const auto expected = static_cast<std::uint8_t>(i & 0xFF);
            if (payload[i] != expected) {
                std::cerr << "Payload pattern mismatch at index " << i
                          << " in sequence " << header.sequence << '\n';
                eventstream::closeSocket(socket);
                eventstream::cleanupSockets();
                return 1;
            }
        }

        if (header.sequence == 1 || header.sequence % 30 == 0) {
            std::cout << "Frame received successfully\n"
                      << "stream_id=" << header.stream_id << '\n'
                      << "sequence=" << header.sequence << '\n'
                      << "payload_size=" << header.payload_size << '\n'
                      << "Payload pattern valid\n";
        }
        ++framesReceived;
        ++expectedSequence;

        if (options.mode == eventstream::ConsumerMode::Slow) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(options.delay_ms));
        }
    }

    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                               streamEnd - streamStart)
                               .count();
    std::cout << "mode="
              << (options.mode == eventstream::ConsumerMode::Fast ? "fast"
                                                                  : "slow")
              << '\n'
              << "id=" << options.id << '\n'
              << "frames_received=" << framesReceived << '\n'
              << "elapsed_ms=" << elapsedMs << '\n';
    eventstream::closeSocket(socket);
    eventstream::cleanupSockets();
    return 0;
}
