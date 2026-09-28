#pragma once

#include <cstdint>
#include <string>

namespace eventstream {

struct ProducerOptions {
    std::string host;
    std::uint16_t port{};
    std::uint32_t fps{};
    std::uint64_t frames{};
};

struct ServerOptions {
    std::uint16_t producer_port{};
    std::uint16_t consumer_port{};
    std::uint32_t expected_consumers{};
};

enum class ConsumerMode {
    Fast,
    Slow,
};

struct ConsumerOptions {
    std::string host;
    std::uint16_t port{};
    ConsumerMode mode{ConsumerMode::Fast};
    std::uint32_t id{};
    std::uint32_t delay_ms{};
};

bool parseProducerOptions(int argc, char* argv[], ProducerOptions& options,
                          std::string& error);
bool parseServerOptions(int argc, char* argv[], ServerOptions& options,
                        std::string& error);
bool parseConsumerOptions(int argc, char* argv[], ConsumerOptions& options,
                          std::string& error);

} // namespace eventstream
