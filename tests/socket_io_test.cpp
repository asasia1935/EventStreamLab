#include <eventstream/socket.hpp>

#if defined(_WIN32)
#include <ws2tcpip.h>
#else
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

#if defined(__linux__)
#include <sys/epoll.h>
#include <unistd.h>
#endif

#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

bool getBoundPort(eventstream::SocketHandle socket, std::uint16_t& port,
                  std::string& error) {
    sockaddr_in address{};
#if defined(_WIN32)
    int addressLength = sizeof(address);
    const int result = getsockname(
        socket, reinterpret_cast<sockaddr*>(&address), &addressLength);
#else
    socklen_t addressLength = sizeof(address);
    const int result = getsockname(
        socket, reinterpret_cast<sockaddr*>(&address), &addressLength);
#endif
    if (result != 0) {
        error = "getsockname() failed while reading ephemeral port";
        return false;
    }
    port = ntohs(address.sin_port);
    return true;
}

using Sender = std::function<bool(eventstream::SocketHandle, std::string&)>;

bool runLoopbackCase(const char* name, std::size_t receiveSize,
                     const Sender& sender, bool expectReceiveSuccess,
                     const std::vector<std::uint8_t>& expectedBytes,
                     bool useStatus = false,
                     eventstream::RecvExactResult expectedStatus =
                         eventstream::RecvExactResult::Success,
                     bool requireError = false) {
    std::string error;
    if (!eventstream::initializeSockets(error)) {
        std::cerr << name << " setup failed: " << error << '\n';
        return false;
    }

    const auto listener = eventstream::createTcpListener(0, error);
    if (!eventstream::isValidSocket(listener)) {
        std::cerr << name << " listener creation failed: " << error << '\n';
        eventstream::cleanupSockets();
        return false;
    }

    std::uint16_t port{};
    if (!getBoundPort(listener, port, error)) {
        std::cerr << name << " port lookup failed: " << error << '\n';
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
        return false;
    }

    const auto client = eventstream::connectTcp("127.0.0.1", port, error);
    if (!eventstream::isValidSocket(client)) {
        std::cerr << name << " client connect failed: " << error << '\n';
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
        return false;
    }

    eventstream::SocketHandle accepted = eventstream::INVALID_SOCKET_HANDLE;
    bool receiveSucceeded = false;
    auto receiveStatus = eventstream::RecvExactResult::Error;
    std::string acceptError;
    std::string receiveError;
    std::vector<std::uint8_t> received(receiveSize);

    std::thread server([&] {
        accepted = eventstream::acceptTcp(listener, acceptError);
        if (!eventstream::isValidSocket(accepted)) {
            return;
        }
        if (useStatus) {
            receiveStatus = eventstream::recvExactWithStatus(
                accepted, received.data(), received.size(), receiveError);
            receiveSucceeded =
                receiveStatus == eventstream::RecvExactResult::Success;
        } else {
            receiveSucceeded = eventstream::recvExact(
                accepted, received.data(), received.size(), receiveError);
        }
        eventstream::closeSocket(accepted);
    });

    const bool sendSucceeded = sender(client, error);
    eventstream::closeSocket(client);
    server.join();

    eventstream::closeSocket(listener);
    eventstream::cleanupSockets();

    if (!sendSucceeded) {
        std::cerr << name << " send failed: " << error << '\n';
        return false;
    }
    if (!acceptError.empty()) {
        std::cerr << name << " accept failed: " << acceptError << '\n';
        return false;
    }
    if (receiveSucceeded != expectReceiveSuccess) {
        std::cerr << name << " had unexpected recvExact result: "
                  << (receiveSucceeded ? "success" : "failure")
                  << '\n';
        if (!receiveError.empty()) {
            std::cerr << receiveError << '\n';
        }
        return false;
    }
    if (useStatus && receiveStatus != expectedStatus) {
        std::cerr << name << " had unexpected status result\n";
        return false;
    }
    if (expectReceiveSuccess && received != expectedBytes) {
        std::cerr << name << " received bytes differ from sent bytes\n";
        return false;
    }
    if (!expectReceiveSuccess && !useStatus && receiveError.empty()) {
        std::cerr << name << " expected a non-empty receive error\n";
        return false;
    }
    if (requireError && receiveError.empty()) {
        std::cerr << name << " expected a non-empty receive error\n";
        return false;
    }

    std::cout << name << " passed\n";
    return true;
}

bool testSendAllRecvExactTransfer() {
    std::vector<std::uint8_t> payload(512 * 1024);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::uint8_t>(i & 0xFF);
    }

    const Sender sender = [&payload](eventstream::SocketHandle socket,
                                     std::string& error) {
        return eventstream::sendAll(socket, payload.data(), payload.size(),
                                    error);
    };
    return runLoopbackCase("sendAll / recvExact Data Transfer Test",
                           payload.size(), sender, true, payload);
}

bool testRecvExactAcrossMultipleWrites() {
    const std::vector<std::uint8_t> expected{
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'};
    const Sender sender = [](eventstream::SocketHandle socket,
                             std::string& error) {
        constexpr std::uint8_t first[]{'A', 'B', 'C', 'D'};
        constexpr std::uint8_t second[]{'E', 'F', 'G', 'H', 'I', 'J'};
        if (!eventstream::sendAll(socket, first, sizeof(first), error)) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return eventstream::sendAll(socket, second, sizeof(second), error);
    };
    return runLoopbackCase("recvExact Across Multiple Writes Test", 10,
                           sender, true, expected);
}

bool testPeerClosesBeforeExactReceive() {
    const Sender sender = [](eventstream::SocketHandle socket,
                             std::string& error) {
        constexpr std::uint8_t partial[]{'A', 'B', 'C', 'D'};
        return eventstream::sendAll(socket, partial, sizeof(partial), error);
    };
    return runLoopbackCase("Peer Close Before Exact Receive Test", 10, sender,
                           false, {}, false,
                           eventstream::RecvExactResult::Success, true) &&
           runLoopbackCase("Partial Receive Status Test", 10, sender, false,
                           {}, true, eventstream::RecvExactResult::Error,
                           true);
}

bool testCleanPeerCloseStatus() {
    const Sender sender = [](eventstream::SocketHandle,
                             std::string&) { return true; };
    return runLoopbackCase("Clean Peer Close Status Test", 34, sender, false,
                           {}, true,
                           eventstream::RecvExactResult::PeerClosed);
}

bool testNonBlockingReceiveWithoutData() {
#if defined(_WIN32)
    std::cout << "Non-blocking POSIX behavior test skipped on Windows\n";
    return true;
#else
    std::string error;
    if (!eventstream::initializeSockets(error)) {
        std::cerr << "Non-blocking receive setup failed: " << error << '\n';
        return false;
    }

    const auto listener = eventstream::createTcpListener(0, error);
    if (!eventstream::isValidSocket(listener)) {
        std::cerr << "Non-blocking test listener creation failed: " << error
                  << '\n';
        eventstream::cleanupSockets();
        return false;
    }

    std::uint16_t port{};
    if (!getBoundPort(listener, port, error)) {
        std::cerr << "Non-blocking test port lookup failed: " << error << '\n';
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
        return false;
    }

    const auto client = eventstream::connectTcp("127.0.0.1", port, error);
    if (!eventstream::isValidSocket(client)) {
        std::cerr << "Non-blocking test client connect failed: " << error
                  << '\n';
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
        return false;
    }

    const auto accepted = eventstream::acceptTcp(listener, error);
    if (!eventstream::isValidSocket(accepted)) {
        std::cerr << "Non-blocking test accept failed: " << error << '\n';
        eventstream::closeSocket(client);
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
        return false;
    }

    if (!eventstream::setNonBlocking(accepted, error)) {
        std::cerr << "setNonBlocking() failed: " << error << '\n';
        eventstream::closeSocket(accepted);
        eventstream::closeSocket(client);
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
        return false;
    }

    std::uint8_t byte{};
    const ssize_t result = ::recv(accepted, &byte, sizeof(byte), 0);
    const int receiveError = errno;

    eventstream::closeSocket(accepted);
    eventstream::closeSocket(client);
    eventstream::closeSocket(listener);
    eventstream::cleanupSockets();

    if (result != -1) {
        std::cerr << "Non-blocking recv() returned " << result
                  << " instead of -1\n";
        return false;
    }
    if (receiveError != EAGAIN && receiveError != EWOULDBLOCK) {
        std::cerr << "Non-blocking recv() returned unexpected errno "
                  << receiveError << '\n';
        return false;
    }

    std::cout << "Non-blocking recv() would-block test passed\n";
    return true;
#endif
}

bool testEpollReadableNotification() {
#if !defined(__linux__)
    std::cout << "Linux epoll readiness test skipped on this platform\n";
    return true;
#else
    std::string error;
    if (!eventstream::initializeSockets(error)) {
        std::cerr << "epoll readiness setup failed: " << error << '\n';
        return false;
    }

    auto listener = eventstream::INVALID_SOCKET_HANDLE;
    auto client = eventstream::INVALID_SOCKET_HANDLE;
    auto accepted = eventstream::INVALID_SOCKET_HANDLE;
    int epollFd = -1;
    const auto cleanup = [&] {
        if (epollFd != -1) {
            close(epollFd);
        }
        eventstream::closeSocket(accepted);
        eventstream::closeSocket(client);
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
    };

    listener = eventstream::createTcpListener(0, error);
    if (!eventstream::isValidSocket(listener)) {
        std::cerr << "epoll test listener creation failed: " << error << '\n';
        cleanup();
        return false;
    }

    std::uint16_t port{};
    if (!getBoundPort(listener, port, error)) {
        std::cerr << "epoll test port lookup failed: " << error << '\n';
        cleanup();
        return false;
    }

    client = eventstream::connectTcp("127.0.0.1", port, error);
    if (!eventstream::isValidSocket(client)) {
        std::cerr << "epoll test client connect failed: " << error << '\n';
        cleanup();
        return false;
    }

    accepted = eventstream::acceptTcp(listener, error);
    if (!eventstream::isValidSocket(accepted)) {
        std::cerr << "epoll test accept failed: " << error << '\n';
        cleanup();
        return false;
    }

    if (!eventstream::setNonBlocking(accepted, error)) {
        std::cerr << "epoll test setNonBlocking() failed: " << error << '\n';
        cleanup();
        return false;
    }

    // epollFd is a file descriptor for the epoll instance itself.
    epollFd = epoll_create1(0);
    if (epollFd == -1) {
        std::cerr << "epoll_create1() failed (errno " << errno << ")\n";
        cleanup();
        return false;
    }

    epoll_event registration{};
    registration.events = EPOLLIN;
    registration.data.fd = accepted;
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, accepted, &registration) == -1) {
        std::cerr << "epoll_ctl(EPOLL_CTL_ADD) failed (errno " << errno
                  << ")\n";
        cleanup();
        return false;
    }

    epoll_event events[4]{};
    const int initiallyReady =
        epoll_wait(epollFd, events, 4, 0);
    if (initiallyReady == -1) {
        std::cerr << "epoll_wait() before send failed (errno " << errno
                  << ")\n";
        cleanup();
        return false;
    }
    if (initiallyReady != 0) {
        std::cerr << "epoll reported an event before client data was sent\n";
        cleanup();
        return false;
    }

    constexpr std::uint8_t expectedByte = 'A';
    if (!eventstream::sendAll(client, &expectedByte, sizeof(expectedByte),
                              error)) {
        std::cerr << "epoll test client send failed: " << error << '\n';
        cleanup();
        return false;
    }

    int readyCount;
    do {
        readyCount = epoll_wait(epollFd, events, 4, 1500);
    } while (readyCount == -1 && errno == EINTR);
    if (readyCount == -1) {
        std::cerr << "epoll_wait() failed (errno " << errno << ")\n";
        cleanup();
        return false;
    }
    if (readyCount == 0) {
        std::cerr << "epoll_wait() timed out waiting for readable socket\n";
        cleanup();
        return false;
    }

    bool acceptedReadable = false;
    for (int i = 0; i < readyCount; ++i) {
        if (events[i].data.fd == accepted &&
            (events[i].events & EPOLLIN) != 0) {
            acceptedReadable = true;
            break;
        }
    }
    if (!acceptedReadable) {
        std::cerr << "epoll_wait() returned no EPOLLIN event for accepted socket\n";
        cleanup();
        return false;
    }

    std::uint8_t receivedByte{};
    const ssize_t received = ::recv(accepted, &receivedByte,
                                    sizeof(receivedByte), 0);
    if (received <= 0) {
        std::cerr << "recv() after EPOLLIN returned " << received;
        if (received == -1) {
            std::cerr << " (errno " << errno << ')';
        }
        std::cerr << '\n';
        cleanup();
        return false;
    }
    if (receivedByte != expectedByte) {
        std::cerr << "recv() returned a byte different from the sent payload\n";
        cleanup();
        return false;
    }

    cleanup();
    std::cout << "epoll EPOLLIN readiness test passed\n";
    return true;
#endif
}

bool testMinimalEpollReadEventLoop() {
#if !defined(__linux__)
    std::cout << "Linux epoll read event loop test skipped on this platform\n";
    return true;
#else
    std::string error;
    if (!eventstream::initializeSockets(error)) {
        std::cerr << "epoll event loop setup failed: " << error << '\n';
        return false;
    }

    auto listener = eventstream::INVALID_SOCKET_HANDLE;
    auto client = eventstream::INVALID_SOCKET_HANDLE;
    auto accepted = eventstream::INVALID_SOCKET_HANDLE;
    int epollFd = -1;
    const auto cleanup = [&] {
        if (epollFd != -1) {
            close(epollFd);
        }
        eventstream::closeSocket(accepted);
        eventstream::closeSocket(client);
        eventstream::closeSocket(listener);
        eventstream::cleanupSockets();
    };

    listener = eventstream::createTcpListener(0, error);
    if (!eventstream::isValidSocket(listener)) {
        std::cerr << "epoll event loop listener creation failed: " << error
                  << '\n';
        cleanup();
        return false;
    }

    std::uint16_t port{};
    if (!getBoundPort(listener, port, error)) {
        std::cerr << "epoll event loop port lookup failed: " << error << '\n';
        cleanup();
        return false;
    }

    client = eventstream::connectTcp("127.0.0.1", port, error);
    if (!eventstream::isValidSocket(client)) {
        std::cerr << "epoll event loop client connect failed: " << error
                  << '\n';
        cleanup();
        return false;
    }

    accepted = eventstream::acceptTcp(listener, error);
    if (!eventstream::isValidSocket(accepted)) {
        std::cerr << "epoll event loop accept failed: " << error << '\n';
        cleanup();
        return false;
    }
    if (!eventstream::setNonBlocking(accepted, error)) {
        std::cerr << "epoll event loop setNonBlocking() failed: " << error
                  << '\n';
        cleanup();
        return false;
    }

    epollFd = epoll_create1(0);
    if (epollFd == -1) {
        std::cerr << "epoll event loop epoll_create1() failed (errno "
                  << errno << ")\n";
        cleanup();
        return false;
    }

    epoll_event registration{};
    registration.events = EPOLLIN;
    registration.data.fd = accepted;
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, accepted, &registration) == -1) {
        std::cerr << "epoll event loop epoll_ctl() failed (errno " << errno
                  << ")\n";
        cleanup();
        return false;
    }

    const std::vector<std::uint8_t> expected{'A', 'B', 'C'};
    if (!eventstream::sendAll(client, expected.data(), expected.size(), error)) {
        std::cerr << "epoll event loop client send failed: " << error << '\n';
        cleanup();
        return false;
    }

    // This state survives each event handler and the next epoll_wait().
    std::vector<std::uint8_t> receivedBytes(expected.size());
    std::size_t receivedCount = 0;
    std::size_t receiveCycles = 0;
    int waitCount = 0;
    constexpr int maxWaitCount = 16; // Safety bound even after no-progress events.
    epoll_event events[4]{};
    while (receivedCount < expected.size() && waitCount < maxWaitCount) {
        ++waitCount;
        const int readyCount = epoll_wait(epollFd, events, 4, 1500);
        if (readyCount == -1) {
            const int code = errno;
            if (code == EINTR) {
                continue;
            }
            std::cerr << "epoll event loop epoll_wait() failed (errno "
                      << code << ")\n";
            cleanup();
            return false;
        }
        if (readyCount == 0) {
            std::cerr << "epoll event loop timed out waiting for data\n";
            cleanup();
            return false;
        }

        for (int i = 0; i < readyCount; ++i) {
            if (events[i].data.fd != accepted ||
                (events[i].events & EPOLLIN) == 0) {
                std::cerr << "epoll event loop returned an unexpected event\n";
                cleanup();
                return false;
            }

            // Read at most one byte per handler to demonstrate repeated LT
            // notifications for remaining data, independently of send boundaries.
            ssize_t received;
            do {
                received = ::recv(accepted,
                                  receivedBytes.data() + receivedCount, 1, 0);
            } while (received == -1 && errno == EINTR);
            if (received > 0) {
                receivedCount += static_cast<std::size_t>(received);
                ++receiveCycles;
            } else if (received == 0) {
                std::cerr << "epoll event loop peer closed before all bytes\n";
                cleanup();
                return false;
            } else {
                const int code = errno;
                if (code == EAGAIN || code == EWOULDBLOCK) {
                    continue;
                }
                std::cerr << "epoll event loop recv() failed (errno " << code
                          << ")\n";
                cleanup();
                return false;
            }
        }
    }

    cleanup();
    if (receivedCount != expected.size() || receivedBytes != expected) {
        std::cerr << "epoll event loop did not receive the complete ABC sequence\n";
        return false;
    }
    if (waitCount < 3 || receiveCycles < 3) {
        std::cerr << "epoll event loop did not repeat wait / receive cycles\n";
        return false;
    }
    std::cout << "Minimal epoll read event loop test passed ("
              << receiveCycles << " receive cycles)\n";
    return true;
#endif
}

} // namespace

int main() {
    if (!testSendAllRecvExactTransfer() ||
        !testRecvExactAcrossMultipleWrites() ||
        !testPeerClosesBeforeExactReceive() || !testCleanPeerCloseStatus() ||
        !testNonBlockingReceiveWithoutData() ||
        !testEpollReadableNotification() ||
        !testMinimalEpollReadEventLoop()) {
        return 1;
    }
    std::cout << "All socket I/O tests passed\n";
    return 0;
}
