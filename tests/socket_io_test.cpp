#include <eventstream/socket.hpp>

#if defined(_WIN32)
#include <ws2tcpip.h>
#else
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
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

} // namespace

int main() {
    if (!testSendAllRecvExactTransfer() ||
        !testRecvExactAcrossMultipleWrites() ||
        !testPeerClosesBeforeExactReceive() || !testCleanPeerCloseStatus() ||
        !testNonBlockingReceiveWithoutData()) {
        return 1;
    }
    std::cout << "All socket I/O tests passed\n";
    return 0;
}
