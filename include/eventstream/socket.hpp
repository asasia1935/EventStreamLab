#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#if defined(_WIN32)
#include <winsock2.h>
#endif

namespace eventstream {

#if defined(_WIN32)
using SocketHandle = SOCKET;
inline constexpr SocketHandle INVALID_SOCKET_HANDLE =
    static_cast<SocketHandle>(-1);
#else
using SocketHandle = int;
inline constexpr SocketHandle INVALID_SOCKET_HANDLE = -1;
#endif

bool initializeSockets(std::string& error);
void cleanupSockets();

bool isValidSocket(SocketHandle socket);
void closeSocket(SocketHandle socket);
bool setNonBlocking(SocketHandle socket, std::string& error);

SocketHandle connectTcp(const std::string& host, std::uint16_t port,
                        std::string& error);
SocketHandle createTcpListener(std::uint16_t port, std::string& error);
SocketHandle acceptTcp(SocketHandle listener, std::string& error);

bool sendAll(SocketHandle socket, const std::uint8_t* data, std::size_t size,
             std::string& error);

enum class RecvExactResult {
    Success,
    PeerClosed,
    Error,
};

RecvExactResult recvExactWithStatus(SocketHandle socket, std::uint8_t* data,
                                    std::size_t size, std::string& error);
bool recvExact(SocketHandle socket, std::uint8_t* data, std::size_t size,
               std::string& error);

} // namespace eventstream
