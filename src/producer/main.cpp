#include <eventstream/cli.hpp>
#include <eventstream/clock.hpp>
#include <eventstream/protocol.hpp>
#include <eventstream/socket.hpp>
#include <eventstream/workload.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

int main(int argc, char* argv[]) {
    eventstream::ProducerOptions options;
    std::string error;
    if (!eventstream::parseProducerOptions(argc, argv, options, error)) {
        std::cerr << error << '\n';
        return 1;
    }

    std::cout << "EventStreamLab Producer\n";
    std::cout << "host=" << options.host << '\n'
              << "port=" << options.port << '\n'
              << "fps=" << options.fps << '\n'
              << "frames=" << options.frames << '\n';

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

    const std::vector<std::uint8_t> payloadBuffer =
        eventstream::makeSyntheticPayloadBuffer();

    using SteadyClock = std::chrono::steady_clock;
    auto period = std::chrono::duration_cast<SteadyClock::duration>(
        std::chrono::seconds(1));
    period /= static_cast<SteadyClock::duration::rep>(options.fps);
    if (period <= SteadyClock::duration::zero()) {
        period = SteadyClock::duration{1};
    }

    const auto producerStart = SteadyClock::now();
    auto deadline = producerStart;
    SteadyClock::duration totalSendTime{};
    SteadyClock::duration maxSendTime{};
    for (std::uint64_t sequence = 1;;) {
        std::this_thread::sleep_until(deadline);

        const std::size_t payloadSize =
            eventstream::payloadSizeForSequence(sequence);
        const eventstream::FrameHeader header{
            eventstream::FRAME_MAGIC,
            eventstream::PROTOCOL_VERSION,
            1,
            sequence,
            eventstream::monotonicNowNs(),
            static_cast<std::uint32_t>(payloadSize),
            0,
        };
        const eventstream::WireHeader wireHeader =
            eventstream::serializeHeader(header);

        if (sequence == 1 || sequence % 30 == 0 ||
            sequence == options.frames) {
            std::cout << "Sending frame\n"
                      << "sequence=" << sequence << '\n'
                      << "payload_size=" << payloadSize << '\n';
        }

        const auto sendStart = SteadyClock::now();
        if (!eventstream::sendAll(socket, wireHeader.data(), wireHeader.size(),
                                  error)) {
            std::cerr << error << '\n';
            eventstream::closeSocket(socket);
            eventstream::cleanupSockets();
            return 1;
        }
        if (!eventstream::sendAll(socket, payloadBuffer.data(), payloadSize,
                                  error)) {
            std::cerr << error << '\n';
            eventstream::closeSocket(socket);
            eventstream::cleanupSockets();
            return 1;
        }
        const auto sendEnd = SteadyClock::now();
        const auto sendDuration = sendEnd - sendStart;
        totalSendTime += sendDuration;
        maxSendTime = (std::max)(maxSendTime, sendDuration);

        if (sequence == options.frames) {
            break;
        }
        ++sequence;
        deadline += period;
    }

    const auto producerEnd = SteadyClock::now();
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                               producerEnd - producerStart)
                               .count();
    const auto totalSendUs =
        std::chrono::duration_cast<std::chrono::microseconds>(totalSendTime)
            .count();
    const auto maxSendUs =
        std::chrono::duration_cast<std::chrono::microseconds>(maxSendTime)
            .count();
    std::cout << "Producer summary\n"
              << "frames_sent=" << options.frames << '\n'
              << "elapsed_ms=" << elapsedMs << '\n'
              << "total_send_us=" << totalSendUs << '\n'
              << "max_send_us=" << maxSendUs << '\n';
    eventstream::closeSocket(socket);
    eventstream::cleanupSockets();
    return 0;
}
