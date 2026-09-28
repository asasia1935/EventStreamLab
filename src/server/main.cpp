#include <eventstream/cli.hpp>
#include <eventstream/protocol.hpp>
#include <eventstream/socket.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <vector>

namespace {

struct ConsumerSendMetrics {
    std::uint64_t frames_forwarded{};
    std::chrono::steady_clock::duration total_send_time{};
    std::chrono::steady_clock::duration max_send_time{};
};

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
    eventstream::ServerOptions options;
    std::string error;
    if (!eventstream::parseServerOptions(argc, argv, options, error)) {
        std::cerr << error << '\n';
        return 1;
    }

    std::cout << "EventStreamLab Server\n";
    std::cout << "producer_port=" << options.producer_port << '\n'
              << "consumer_port=" << options.consumer_port << '\n'
              << "expected_consumers=" << options.expected_consumers << '\n';

    if (!eventstream::initializeSockets(error)) {
        std::cerr << error << '\n';
        return 1;
    }

    eventstream::SocketHandle producerListener =
        eventstream::INVALID_SOCKET_HANDLE;
    eventstream::SocketHandle consumerListener =
        eventstream::INVALID_SOCKET_HANDLE;
    eventstream::SocketHandle producerSocket =
        eventstream::INVALID_SOCKET_HANDLE;
    std::vector<eventstream::SocketHandle> consumerSockets;
    consumerSockets.reserve(options.expected_consumers);
    std::vector<ConsumerSendMetrics> consumerMetrics;
    consumerMetrics.reserve(options.expected_consumers);

    const auto cleanup = [&] {
        eventstream::closeSocket(producerSocket);
        for (const auto socket : consumerSockets) {
            eventstream::closeSocket(socket);
        }
        eventstream::closeSocket(producerListener);
        eventstream::closeSocket(consumerListener);
        eventstream::cleanupSockets();
    };

    producerListener =
        eventstream::createTcpListener(options.producer_port, error);
    if (!eventstream::isValidSocket(producerListener)) {
        std::cerr << error << '\n';
        cleanup();
        return 1;
    }

    consumerListener =
        eventstream::createTcpListener(options.consumer_port, error);
    if (!eventstream::isValidSocket(consumerListener)) {
        std::cerr << error << '\n';
        cleanup();
        return 1;
    }

    std::cout << "Listening for producer on port " << options.producer_port
              << '\n'
              << "Listening for consumers on port " << options.consumer_port
              << '\n'
              << "Producer connection waiting...\n";

    producerSocket = eventstream::acceptTcp(producerListener, error);
    if (!eventstream::isValidSocket(producerSocket)) {
        std::cerr << error << '\n';
        cleanup();
        return 1;
    }
    std::cout << "Producer connected\n";

    for (std::uint32_t i = 0; i < options.expected_consumers; ++i) {
        std::cout << "Consumer connection waiting...\n";
        const eventstream::SocketHandle consumerSocket =
            eventstream::acceptTcp(consumerListener, error);
        if (!eventstream::isValidSocket(consumerSocket)) {
            std::cerr << error << '\n';
            cleanup();
            return 1;
        }
        consumerSockets.push_back(consumerSocket);
        consumerMetrics.emplace_back();
        std::cout << "Consumer connection " << (i + 1) << '/'
                  << options.expected_consumers << " accepted\n";
    }

    std::cout << "All expected connections established\n";

    std::uint64_t expectedSequence = 1;
    std::uint64_t framesReceived = 0;
    const auto serverStreamStart = std::chrono::steady_clock::now();
    auto serverStreamEnd = serverStreamStart;
    for (;;) {
        eventstream::WireHeader wireHeader{};
        const auto receiveResult = eventstream::recvExactWithStatus(
            producerSocket, wireHeader.data(), wireHeader.size(), error);
        if (receiveResult == eventstream::RecvExactResult::PeerClosed) {
            std::cout << "Producer stream ended normally\n";
            serverStreamEnd = std::chrono::steady_clock::now();
            break;
        }
        if (receiveResult == eventstream::RecvExactResult::Error) {
            std::cerr << "Failed to receive frame header: " << error << '\n';
            cleanup();
            return 1;
        }

        const eventstream::FrameHeader header =
            eventstream::deserializeHeader(wireHeader);
        const auto validation = eventstream::validateHeader(header);
        if (validation != eventstream::HeaderValidationError::None) {
            std::cerr << validationErrorMessage(validation) << '\n';
            cleanup();
            return 1;
        }
        if (header.sequence != expectedSequence) {
            std::cerr << "Frame sequence mismatch: expected "
                      << expectedSequence << ", got " << header.sequence
                      << '\n';
            cleanup();
            return 1;
        }

        std::vector<std::uint8_t> payload(header.payload_size);
        if (!eventstream::recvExact(producerSocket, payload.data(),
                                    payload.size(), error)) {
            std::cerr << "Failed to receive frame payload: " << error << '\n';
            cleanup();
            return 1;
        }

        for (std::size_t i = 0; i < payload.size(); ++i) {
            const auto expected = static_cast<std::uint8_t>(i & 0xFF);
            if (payload[i] != expected) {
                std::cerr << "Payload pattern mismatch at index " << i
                          << " in sequence " << header.sequence << '\n';
                cleanup();
                return 1;
            }
        }

        for (std::size_t i = 0; i < consumerSockets.size(); ++i) {
            if (header.sequence == 1 || header.sequence % 30 == 0) {
                std::cout << "Forwarding frame sequence=" << header.sequence
                          << " to consumer " << (i + 1) << '/'
                          << consumerSockets.size() << '\n';
            }
            const auto sendStart = std::chrono::steady_clock::now();
            if (!eventstream::sendAll(consumerSockets[i], wireHeader.data(),
                                      wireHeader.size(), error)) {
                std::cerr << "Failed forwarding header to consumer connection "
                          << (i + 1) << ": " << error << '\n';
                cleanup();
                return 1;
            }
            if (!eventstream::sendAll(consumerSockets[i], payload.data(),
                                      payload.size(), error)) {
                std::cerr << "Failed forwarding payload to consumer connection "
                          << (i + 1) << ": " << error << '\n';
                cleanup();
                return 1;
            }
            const auto sendEnd = std::chrono::steady_clock::now();
            const auto forwardDuration = sendEnd - sendStart;
            auto& metrics = consumerMetrics[i];
            ++metrics.frames_forwarded;
            metrics.total_send_time += forwardDuration;
            metrics.max_send_time =
                (std::max)(metrics.max_send_time, forwardDuration);
        }

        if (header.sequence == 1 || header.sequence % 30 == 0) {
            std::cout << "Frame received and forwarded\n"
                      << "stream_id=" << header.stream_id << '\n'
                      << "sequence=" << header.sequence << '\n'
                      << "payload_size=" << header.payload_size << '\n';
        }

        ++framesReceived;
        ++expectedSequence;
    }

    const auto serverElapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            serverStreamEnd - serverStreamStart)
            .count();
    std::cout << "Server summary\n"
              << "frames_received=" << framesReceived << '\n'
              << "elapsed_ms=" << serverElapsedMs << '\n';
    for (std::size_t i = 0; i < consumerMetrics.size(); ++i) {
        const auto totalSendUs =
            std::chrono::duration_cast<std::chrono::microseconds>(
                consumerMetrics[i].total_send_time)
                .count();
        const auto maxSendUs =
            std::chrono::duration_cast<std::chrono::microseconds>(
                consumerMetrics[i].max_send_time)
                .count();
        std::cout << "consumer_connection=" << (i + 1) << '\n'
                  << "frames_forwarded="
                  << consumerMetrics[i].frames_forwarded << '\n'
                  << "total_send_us=" << totalSendUs << '\n'
                  << "max_send_us=" << maxSendUs << '\n';
    }
    cleanup();
    return 0;
}
