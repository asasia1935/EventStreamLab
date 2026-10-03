#include <eventstream/cli.hpp>
#include <eventstream/protocol.hpp>
#include <eventstream/socket.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <vector>

#if defined(__linux__)
#include <cerrno>
#include <deque>
#include <memory>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

struct ConsumerSendMetrics {
    std::uint64_t frames_forwarded{};
    std::chrono::steady_clock::duration total_send_time{};
    std::chrono::steady_clock::duration max_send_time{};
#if defined(__linux__)
    std::uint64_t frames_enqueued{};
    std::uint64_t bytes_sent{};
    std::uint64_t partial_writes{};
    std::uint64_t eagain_count{};
    std::uint64_t epollout_wakes{};
    std::size_t max_pending_frames{};
    std::size_t max_pending_bytes{};
#endif
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

#if defined(__linux__)

enum class RxPhase { Header, Payload };

struct ProducerRxState {
    RxPhase phase = RxPhase::Header;
    eventstream::WireHeader wire_header{};
    std::size_t header_offset{};
    eventstream::FrameHeader header{};
    // Header bytes followed by payload bytes, received directly into this buffer.
    std::vector<std::uint8_t> frame_bytes;
    std::size_t payload_offset{};
};

using SharedFrame = std::shared_ptr<const std::vector<std::uint8_t>>;

struct ConsumerTxState {
    std::deque<SharedFrame> pending_frames;
    std::size_t front_offset{};
    std::size_t pending_bytes{}; // Bytes still awaiting send(), including the front.
    bool writable_interest_enabled{};
    std::chrono::steady_clock::duration front_send_time{};
};

std::string linuxIoError(const char* operation, int code) {
    return std::string(operation) + " failed (error " + std::to_string(code) + ")";
}

void closeConsumer(int epollFd, eventstream::SocketHandle& socket,
                   ConsumerTxState& tx, std::size_t index,
                   const std::string& reason) {
    std::cout << "Consumer connection " << (index + 1) << ": " << reason << '\n';
    // close() also removes the fd from epoll; DEL makes the lifecycle explicit.
    if (epoll_ctl(epollFd, EPOLL_CTL_DEL, socket, nullptr) == -1) {
        std::cerr << linuxIoError("epoll_ctl(consumer DEL)", errno) << '\n';
    }
    eventstream::closeSocket(socket);
    socket = eventstream::INVALID_SOCKET_HANDLE;
    tx.pending_frames.clear();
    tx.front_offset = 0;
    tx.pending_bytes = 0;
    tx.front_send_time = {};
    tx.writable_interest_enabled = false;
}

bool setWritableInterest(int epollFd, eventstream::SocketHandle socket,
                         ConsumerTxState& tx, bool enabled, std::string& error) {
    if (tx.writable_interest_enabled == enabled) {
        return true;
    }
    epoll_event interest{};
    interest.events = EPOLLRDHUP | (enabled ? EPOLLOUT : 0);
    interest.data.fd = socket;
    if (epoll_ctl(epollFd, EPOLL_CTL_MOD, socket, &interest) == -1) {
        error = linuxIoError("epoll_ctl(consumer MOD)", errno);
        return false;
    }
    tx.writable_interest_enabled = enabled;
    return true;
}

bool flushConsumerTx(int epollFd, eventstream::SocketHandle& socket,
                     ConsumerTxState& tx, ConsumerSendMetrics& metrics,
                     std::size_t index, std::string& error) {
    while (!tx.pending_frames.empty()) {
        const auto& frame = *tx.pending_frames.front();
        const std::size_t remaining = frame.size() - tx.front_offset;
        const auto sendStart = std::chrono::steady_clock::now();
        const ssize_t sent = ::send(socket, frame.data() + tx.front_offset,
                                   remaining, MSG_NOSIGNAL);
        const int code = sent < 0 ? errno : 0;
        const auto duration = std::chrono::steady_clock::now() - sendStart;
        // Active send() time only; time waiting for EPOLLOUT is excluded.
        metrics.total_send_time += duration;
        tx.front_send_time += duration;
        if (sent > 0) {
            const auto progress = static_cast<std::size_t>(sent);
            metrics.bytes_sent += progress;
            metrics.partial_writes += progress < remaining;
            tx.front_offset += progress;
            tx.pending_bytes -= progress;
            if (tx.front_offset == frame.size()) {
                ++metrics.frames_forwarded;
                metrics.max_send_time =
                    (std::max)(metrics.max_send_time, tx.front_send_time);
                tx.pending_frames.pop_front();
                tx.front_offset = 0;
                tx.front_send_time = {};
            }
            continue;
        }
        if (sent < 0 && code == EINTR) {
            continue;
        }
        if (sent < 0 && (code == EAGAIN || code == EWOULDBLOCK)) {
            ++metrics.eagain_count;
            return setWritableInterest(epollFd, socket, tx, true, error);
        }
        closeConsumer(epollFd, socket, tx, index,
                      sent == 0 ? "send() made no progress"
                                : linuxIoError("send()", code));
        return true; // A failed Consumer does not terminate the other streams.
    }
    return setWritableInterest(epollFd, socket, tx, false, error);
}

bool processProducerRx(int epollFd, eventstream::SocketHandle producerSocket,
                       ProducerRxState& rx, bool& producerFinished,
                       std::vector<eventstream::SocketHandle>& consumerSockets,
                       std::vector<ConsumerTxState>& consumerTx,
                       std::vector<ConsumerSendMetrics>& consumerMetrics,
                       std::uint64_t& framesReceived, std::string& error) {
    for (;;) {
        if (rx.phase == RxPhase::Header &&
            rx.header_offset == rx.wire_header.size()) {
            rx.header = eventstream::deserializeHeader(rx.wire_header);
            const auto validation = eventstream::validateHeader(rx.header);
            if (validation != eventstream::HeaderValidationError::None) {
                error = validationErrorMessage(validation);
                return false;
            }
            const auto expectedSequence = framesReceived + 1;
            if (rx.header.sequence != expectedSequence) {
                error = "Frame sequence mismatch: expected " +
                        std::to_string(expectedSequence) + ", got " +
                        std::to_string(rx.header.sequence);
                return false;
            }
            rx.frame_bytes.resize(rx.wire_header.size() + rx.header.payload_size);
            std::copy(rx.wire_header.begin(), rx.wire_header.end(),
                      rx.frame_bytes.begin());
            rx.phase = RxPhase::Payload;
        }
        if (rx.phase == RxPhase::Payload &&
            rx.payload_offset == rx.header.payload_size) {
            for (std::size_t i = 0; i < rx.header.payload_size; ++i) {
                if (rx.frame_bytes[rx.wire_header.size() + i] !=
                    static_cast<std::uint8_t>(i & 0xFF)) {
                    error = "Payload pattern mismatch at index " +
                            std::to_string(i) + " in sequence " +
                            std::to_string(rx.header.sequence);
                    return false;
                }
            }
            const SharedFrame frame =
                std::make_shared<const std::vector<std::uint8_t>>(
                    std::move(rx.frame_bytes));
            for (std::size_t i = 0; i < consumerSockets.size(); ++i) {
                if (!eventstream::isValidSocket(consumerSockets[i])) {
                    continue;
                }
                auto& tx = consumerTx[i];
                auto& metrics = consumerMetrics[i];
                const bool wasEmpty = tx.pending_frames.empty();
                tx.pending_frames.push_back(frame); // Intentionally unbounded.
                tx.pending_bytes += frame->size();
                ++metrics.frames_enqueued;
                metrics.max_pending_frames =
                    (std::max)(metrics.max_pending_frames, tx.pending_frames.size());
                metrics.max_pending_bytes =
                    (std::max)(metrics.max_pending_bytes, tx.pending_bytes);
                if (rx.header.sequence == 1 || rx.header.sequence % 30 == 0) {
                    std::cout << "Queueing frame sequence=" << rx.header.sequence
                              << " to consumer " << (i + 1) << '/'
                              << consumerSockets.size() << '\n';
                }
                if (wasEmpty && !flushConsumerTx(epollFd, consumerSockets[i], tx,
                                                 metrics, i, error)) {
                    return false;
                }
            }
            if (rx.header.sequence == 1 || rx.header.sequence % 30 == 0) {
                std::cout << "Frame received and queued\n"
                          << "stream_id=" << rx.header.stream_id << '\n'
                          << "sequence=" << rx.header.sequence << '\n'
                          << "payload_size=" << rx.header.payload_size << '\n';
            }
            ++framesReceived;
            rx = ProducerRxState{};
            // One complete frame per handler gives other ready fds a turn.
            // Level Triggered epoll reports any remaining Producer bytes again.
            return true;
        }

        const bool readingHeader = rx.phase == RxPhase::Header;
        auto* destination = readingHeader
                                ? rx.wire_header.data() + rx.header_offset
                                : rx.frame_bytes.data() + rx.wire_header.size() +
                                      rx.payload_offset;
        const std::size_t remaining = readingHeader
                                         ? rx.wire_header.size() - rx.header_offset
                                         : rx.header.payload_size - rx.payload_offset;
        const ssize_t received = ::recv(producerSocket, destination, remaining, 0);
        if (received > 0) {
            auto& offset = readingHeader ? rx.header_offset : rx.payload_offset;
            offset += static_cast<std::size_t>(received);
            continue;
        }
        if (received == 0) {
            if (!readingHeader || rx.header_offset != 0) {
                error = readingHeader ? "Producer EOF in incomplete frame header"
                                      : "Producer EOF in incomplete frame payload";
                return false;
            }
            if (epoll_ctl(epollFd, EPOLL_CTL_DEL, producerSocket, nullptr) == -1) {
                error = linuxIoError("epoll_ctl(producer DEL)", errno);
                return false;
            }
            producerFinished = true;
            std::cout << "Producer stream ended normally; draining Consumer TX\n";
            return true;
        }
        const int code = errno;
        if (code == EINTR) {
            continue;
        }
        if (code == EAGAIN || code == EWOULDBLOCK) {
            return true; // Keep RX phase and offsets for the next EPOLLIN.
        }
        error = linuxIoError("recv(producer)", code);
        return false;
    }
}

bool runLinuxEventLoop(eventstream::SocketHandle producerSocket,
                       std::vector<eventstream::SocketHandle>& consumerSockets,
                       std::vector<ConsumerSendMetrics>& consumerMetrics,
                       std::uint64_t& framesReceived, std::string& error) {
    const int epollFd = epoll_create1(0);
    if (epollFd == -1) {
        error = linuxIoError("epoll_create1()", errno);
        return false;
    }
    const auto fail = [&] {
        close(epollFd);
        return false;
    };
    if (!eventstream::setNonBlocking(producerSocket, error)) {
        return fail();
    }
    epoll_event interest{};
    interest.events = EPOLLIN;
    interest.data.fd = producerSocket;
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, producerSocket, &interest) == -1) {
        error = linuxIoError("epoll_ctl(producer ADD)", errno);
        return fail();
    }
    std::vector<ConsumerTxState> consumerTx(consumerSockets.size());
    for (const auto socket : consumerSockets) {
        if (!eventstream::setNonBlocking(socket, error)) {
            return fail();
        }
        interest = {};
        // Consumers never send application data; detect their departure even
        // when there is no pending TX. EPOLLOUT starts disabled.
        interest.events = EPOLLRDHUP;
        interest.data.fd = socket;
        if (epoll_ctl(epollFd, EPOLL_CTL_ADD, socket, &interest) == -1) {
            error = linuxIoError("epoll_ctl(consumer ADD)", errno);
            return fail();
        }
    }
    std::cout << "Linux single-threaded epoll event loop started\n";
    ProducerRxState rx;
    bool producerFinished = false;
    for (;;) {
        if (producerFinished) {
            bool allDrained = true;
            for (std::size_t i = 0; i < consumerSockets.size(); ++i) {
                if (!consumerTx[i].pending_frames.empty()) {
                    allDrained = false;
                } else if (eventstream::isValidSocket(consumerSockets[i])) {
                    closeConsumer(epollFd, consumerSockets[i], consumerTx[i], i,
                                  "stream completed");
                }
            }
            if (allDrained) {
                break;
            }
        }
        epoll_event events[16]{};
        const int readyCount = epoll_wait(epollFd, events, 16, -1);
        if (readyCount == -1) {
            if (errno == EINTR) {
                continue;
            }
            error = linuxIoError("epoll_wait()", errno);
            return fail();
        }
        for (int i = 0; i < readyCount; ++i) {
            const auto socket = events[i].data.fd;
            const auto mask = events[i].events;
            if (socket == producerSocket && !producerFinished) {
                if ((mask & (EPOLLIN | EPOLLHUP | EPOLLERR)) != 0 &&
                    !processProducerRx(epollFd, producerSocket, rx,
                                       producerFinished, consumerSockets,
                                       consumerTx, consumerMetrics, framesReceived,
                                       error)) {
                    return fail();
                }
                continue;
            }
            const auto found = std::find(consumerSockets.begin(),
                                         consumerSockets.end(), socket);
            if (found == consumerSockets.end()) {
                continue; // A previous handler may already have closed this fd.
            }
            const auto index = static_cast<std::size_t>(found - consumerSockets.begin());
            if ((mask & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
                closeConsumer(epollFd, *found, consumerTx[index], index,
                              "peer disconnected");
            } else if ((mask & EPOLLOUT) != 0) {
                ++consumerMetrics[index].epollout_wakes;
                if (!flushConsumerTx(epollFd, *found, consumerTx[index],
                                     consumerMetrics[index], index, error)) {
                    return fail();
                }
            }
        }
    }
    close(epollFd);
    return true;
}

#endif

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

    std::uint64_t framesReceived = 0;
    const auto serverStreamStart = std::chrono::steady_clock::now();
    auto serverStreamEnd = serverStreamStart;
#if defined(__linux__)
    if (!runLinuxEventLoop(producerSocket, consumerSockets, consumerMetrics,
                           framesReceived, error)) {
        std::cerr << error << '\n';
        cleanup();
        return 1;
    }
    serverStreamEnd = std::chrono::steady_clock::now();
#else
    std::uint64_t expectedSequence = 1;
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
#endif

    const auto serverElapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            serverStreamEnd - serverStreamStart)
            .count();
    std::cout << "Server summary\n"
              << "frames_received=" << framesReceived << '\n'
              << "elapsed_ms=" << serverElapsedMs << '\n';
#if defined(__linux__)
    std::cout << "send_time_scope=active_send_calls (max is per completed frame)\n";
#endif
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
#if defined(__linux__)
        const auto& metrics = consumerMetrics[i];
        std::cout << "frames_enqueued=" << metrics.frames_enqueued << '\n'
                  << "frames_completed=" << metrics.frames_forwarded << '\n'
                  << "bytes_sent=" << metrics.bytes_sent << '\n'
                  << "partial_writes=" << metrics.partial_writes << '\n'
                  << "eagain_count=" << metrics.eagain_count << '\n'
                  << "epollout_wakes=" << metrics.epollout_wakes << '\n'
                  << "max_pending_frames=" << metrics.max_pending_frames << '\n'
                  << "max_pending_bytes=" << metrics.max_pending_bytes << '\n';
#endif
    }
    cleanup();
    return 0;
}
