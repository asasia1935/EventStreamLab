#include <eventstream/socket.hpp>

#if defined(_WIN32)
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <cstdint>
#include <limits>
#include <string>

namespace {

std::string socketError(const char* operation, int code) {
    return std::string(operation) + " failed (error " +
           std::to_string(code) + ")";
}

int lastSocketError() {
#if defined(_WIN32)
    return WSAGetLastError();
#else
    return errno;
#endif
}

} // namespace

namespace eventstream {

bool initializeSockets(std::string& error) {
    error.clear();
#if defined(_WIN32)
    WSADATA data{};
    const int result = WSAStartup(MAKEWORD(2, 2), &data);
    if (result != 0) {
        error = socketError("WSAStartup()", result);
        return false;
    }
#endif
    return true;
}

void cleanupSockets() {
#if defined(_WIN32)
    WSACleanup();
#endif
}

bool isValidSocket(SocketHandle socket) {
    return socket != INVALID_SOCKET_HANDLE;
}

void closeSocket(SocketHandle socket) {
    if (!isValidSocket(socket)) {
        return;
    }
#if defined(_WIN32)
    closesocket(socket);
#else
    close(socket);
#endif
}

bool setNonBlocking(SocketHandle socketHandle, std::string& error) {
    error.clear();
    if (!isValidSocket(socketHandle)) {
        error = "setNonBlocking() failed: invalid socket";
        return false;
    }

#if defined(_WIN32)
    u_long nonBlocking = 1;
    if (ioctlsocket(socketHandle, FIONBIO, &nonBlocking) != 0) {
        error = socketError("ioctlsocket(FIONBIO)", lastSocketError());
        return false;
    }
#else
    int flags;
    do {
        flags = fcntl(socketHandle, F_GETFL, 0);
    } while (flags == -1 && errno == EINTR);
    if (flags == -1) {
        error = socketError("fcntl(F_GETFL)", errno);
        return false;
    }

    int result;
    do {
        result = fcntl(socketHandle, F_SETFL, flags | O_NONBLOCK);
    } while (result == -1 && errno == EINTR);
    if (result == -1) {
        error = socketError("fcntl(F_SETFL)", errno);
        return false;
    }
#endif
    return true;
}

SocketHandle connectTcp(const std::string& host, std::uint16_t port,
                        std::string& error) {
    error.clear();
    const SocketHandle socketHandle =
        socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (!isValidSocket(socketHandle)) {
        error = socketError("socket()", lastSocketError());
        return INVALID_SOCKET_HANDLE;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    const int addressResult = inet_pton(AF_INET, host.c_str(), &address.sin_addr);
    if (addressResult != 1) {
        error = addressResult == 0
                    ? "inet_pton() failed: host is not a valid IPv4 address"
                    : socketError("inet_pton()", lastSocketError());
        closeSocket(socketHandle);
        return INVALID_SOCKET_HANDLE;
    }

#if defined(_WIN32)
    const int result = connect(socketHandle,
                               reinterpret_cast<const sockaddr*>(&address),
                               static_cast<int>(sizeof(address)));
#else
    const int result = connect(socketHandle,
                               reinterpret_cast<const sockaddr*>(&address),
                               static_cast<socklen_t>(sizeof(address)));
#endif
    if (result != 0) {
        error = socketError("connect()", lastSocketError());
        closeSocket(socketHandle);
        return INVALID_SOCKET_HANDLE;
    }

    return socketHandle;
}

SocketHandle createTcpListener(std::uint16_t port, std::string& error) {
    error.clear();
    const SocketHandle socketHandle =
        socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (!isValidSocket(socketHandle)) {
        error = socketError("socket()", lastSocketError());
        return INVALID_SOCKET_HANDLE;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);

#if defined(_WIN32)
    const int bindResult = bind(socketHandle,
                                reinterpret_cast<const sockaddr*>(&address),
                                static_cast<int>(sizeof(address)));
#else
    const int bindResult = bind(socketHandle,
                                reinterpret_cast<const sockaddr*>(&address),
                                static_cast<socklen_t>(sizeof(address)));
#endif
    if (bindResult != 0) {
        error = socketError("bind()", lastSocketError());
        closeSocket(socketHandle);
        return INVALID_SOCKET_HANDLE;
    }

    if (listen(socketHandle, SOMAXCONN) != 0) {
        error = socketError("listen()", lastSocketError());
        closeSocket(socketHandle);
        return INVALID_SOCKET_HANDLE;
    }

    return socketHandle;
}

SocketHandle acceptTcp(SocketHandle listener, std::string& error) {
    error.clear();
    if (!isValidSocket(listener)) {
        error = "accept() failed: invalid listener socket";
        return INVALID_SOCKET_HANDLE;
    }

    const SocketHandle connectedSocket = accept(listener, nullptr, nullptr);
    if (!isValidSocket(connectedSocket)) {
        error = socketError("accept()", lastSocketError());
        return INVALID_SOCKET_HANDLE;
    }
    return connectedSocket;
}

bool sendAll(SocketHandle socketHandle, const std::uint8_t* data,
             std::size_t size, std::string& error) {
    error.clear();
    if (!isValidSocket(socketHandle)) {
        error = "send() failed: invalid socket";
        return false;
    }

    std::size_t sent = 0;
    while (sent < size) {
#if defined(_WIN32)
        const std::size_t chunk = (size - sent) <
                                          static_cast<std::size_t>(
                                              (std::numeric_limits<int>::max)())
                                      ? size - sent
                                      : static_cast<std::size_t>(
                                            (std::numeric_limits<int>::max)());
        const int result = ::send(
            socketHandle, reinterpret_cast<const char*>(data + sent),
            static_cast<int>(chunk), 0);
        if (result == SOCKET_ERROR) {
            const int code = WSAGetLastError();
            if (code == WSAEINTR) {
                continue;
            }
            error = socketError("send()", code);
            return false;
        }
#else
        const std::size_t chunk = (size - sent) <
                                          static_cast<std::size_t>(
                                              std::numeric_limits<ssize_t>::max())
                                      ? size - sent
                                      : static_cast<std::size_t>(
                                            std::numeric_limits<ssize_t>::max());
#ifdef MSG_NOSIGNAL
        constexpr int sendFlags = MSG_NOSIGNAL;
#else
        constexpr int sendFlags = 0;
#endif
        const ssize_t result = ::send(socketHandle, data + sent, chunk, sendFlags);
        if (result < 0) {
            const int code = errno;
            if (code == EINTR) {
                continue;
            }
            error = socketError("send()", code);
            return false;
        }
#endif
        if (result == 0) {
            error = "send() returned 0 before all requested bytes were sent";
            return false;
        }
        sent += static_cast<std::size_t>(result);
    }
    return true;
}

RecvExactResult recvExactWithStatus(SocketHandle socketHandle,
                                    std::uint8_t* data, std::size_t size,
                                    std::string& error) {
    error.clear();
    if (!isValidSocket(socketHandle)) {
        error = "recv() failed: invalid socket";
        return RecvExactResult::Error;
    }

    std::size_t received = 0;
    while (received < size) {
#if defined(_WIN32)
        const std::size_t chunk = (size - received) <
                                          static_cast<std::size_t>(
                                              (std::numeric_limits<int>::max)())
                                      ? size - received
                                      : static_cast<std::size_t>(
                                            (std::numeric_limits<int>::max)());
        const int result = ::recv(
            socketHandle, reinterpret_cast<char*>(data + received),
            static_cast<int>(chunk), 0);
        if (result == SOCKET_ERROR) {
            const int code = WSAGetLastError();
            if (code == WSAEINTR) {
                continue;
            }
            error = socketError("recv()", code);
            return RecvExactResult::Error;
        }
#else
        const std::size_t chunk = (size - received) <
                                          static_cast<std::size_t>(
                                              std::numeric_limits<ssize_t>::max())
                                      ? size - received
                                      : static_cast<std::size_t>(
                                            std::numeric_limits<ssize_t>::max());
        const ssize_t result = ::recv(socketHandle, data + received, chunk, 0);
        if (result < 0) {
            const int code = errno;
            if (code == EINTR) {
                continue;
            }
            error = socketError("recv()", code);
            return RecvExactResult::Error;
        }
#endif
        if (result == 0) {
            if (received == 0) {
                return RecvExactResult::PeerClosed;
            }
            error = "Peer closed connection after receiving " +
                    std::to_string(received) + " of " +
                    std::to_string(size) + " requested bytes";
            return RecvExactResult::Error;
        }
        received += static_cast<std::size_t>(result);
    }
    return RecvExactResult::Success;
}

bool recvExact(SocketHandle socketHandle, std::uint8_t* data,
               std::size_t size, std::string& error) {
    const RecvExactResult result =
        recvExactWithStatus(socketHandle, data, size, error);
    if (result == RecvExactResult::Success) {
        return true;
    }
    if (result == RecvExactResult::PeerClosed) {
        error = "Peer closed connection before all requested bytes were received";
    }
    return false;
}

} // namespace eventstream
