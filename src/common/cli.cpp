#include <eventstream/cli.hpp>

#include <charconv>
#include <cstdint>
#include <string_view>

namespace {

template <typename Integer>
bool parseInteger(std::string_view text, Integer& value) {
    if (text.empty()) {
        return false;
    }
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}

bool parsePort(std::string_view text, std::uint16_t& port) {
    std::uint32_t value{};
    if (!parseInteger(text, value) || value == 0 || value > 65535) {
        return false;
    }
    port = static_cast<std::uint16_t>(value);
    return true;
}

bool missingValue(const char* option, std::string& error) {
    error = std::string("Missing value for ") + option;
    return false;
}

bool unknownOption(std::string_view option, std::string& error) {
    error = "Unknown option: ";
    error += option;
    return false;
}

} // namespace

namespace eventstream {

bool parseProducerOptions(int argc, char* argv[], ProducerOptions& options,
                          std::string& error) {
    error.clear();
    bool haveHost = false;
    bool havePort = false;
    bool haveFps = false;
    bool haveFrames = false;

    for (int i = 1; i < argc; ++i) {
        const std::string_view option(argv[i]);
        if (option != "--host" && option != "--port" && option != "--fps" &&
            option != "--frames") {
            return unknownOption(option, error);
        }
        if (i + 1 >= argc) {
            return missingValue(argv[i], error);
        }
        const std::string_view value(argv[++i]);

        if (option == "--host") {
            options.host = value;
            haveHost = true;
        } else if (option == "--port") {
            if (!parsePort(value, options.port)) {
                error = "Invalid --port: expected an integer from 1 to 65535";
                return false;
            }
            havePort = true;
        } else if (option == "--fps") {
            if (!parseInteger(value, options.fps) || options.fps == 0) {
                error = "Invalid --fps: expected a positive integer";
                return false;
            }
            haveFps = true;
        } else if (option == "--frames") {
            if (!parseInteger(value, options.frames) || options.frames == 0) {
                error = "Invalid --frames: expected a positive integer";
                return false;
            }
            haveFrames = true;
        } else {
            return unknownOption(option, error);
        }
    }

    if (!haveHost || !havePort || !haveFps || !haveFrames) {
        error = "Required options: --host --port --fps --frames";
        return false;
    }
    return true;
}

bool parseServerOptions(int argc, char* argv[], ServerOptions& options,
                        std::string& error) {
    error.clear();
    bool haveProducerPort = false;
    bool haveConsumerPort = false;
    bool haveExpectedConsumers = false;

    for (int i = 1; i < argc; ++i) {
        const std::string_view option(argv[i]);
        if (option != "--producer-port" && option != "--consumer-port" &&
            option != "--expected-consumers") {
            return unknownOption(option, error);
        }
        if (i + 1 >= argc) {
            return missingValue(argv[i], error);
        }
        const std::string_view value(argv[++i]);

        if (option == "--producer-port") {
            if (!parsePort(value, options.producer_port)) {
                error = "Invalid --producer-port: expected an integer from 1 to 65535";
                return false;
            }
            haveProducerPort = true;
        } else if (option == "--consumer-port") {
            if (!parsePort(value, options.consumer_port)) {
                error = "Invalid --consumer-port: expected an integer from 1 to 65535";
                return false;
            }
            haveConsumerPort = true;
        } else if (option == "--expected-consumers") {
            if (!parseInteger(value, options.expected_consumers) ||
                options.expected_consumers == 0) {
                error = "Invalid --expected-consumers: expected a positive integer";
                return false;
            }
            haveExpectedConsumers = true;
        } else {
            return unknownOption(option, error);
        }
    }

    if (!haveProducerPort || !haveConsumerPort || !haveExpectedConsumers) {
        error = "Required options: --producer-port --consumer-port --expected-consumers";
        return false;
    }
    return true;
}

bool parseConsumerOptions(int argc, char* argv[], ConsumerOptions& options,
                          std::string& error) {
    error.clear();
    bool haveHost = false;
    bool havePort = false;
    bool haveMode = false;
    bool haveId = false;
    bool haveDelay = false;

    for (int i = 1; i < argc; ++i) {
        const std::string_view option(argv[i]);
        if (option != "--host" && option != "--port" && option != "--mode" &&
            option != "--id" && option != "--delay-ms") {
            return unknownOption(option, error);
        }
        if (i + 1 >= argc) {
            return missingValue(argv[i], error);
        }
        const std::string_view value(argv[++i]);

        if (option == "--host") {
            options.host = value;
            haveHost = true;
        } else if (option == "--port") {
            if (!parsePort(value, options.port)) {
                error = "Invalid --port: expected an integer from 1 to 65535";
                return false;
            }
            havePort = true;
        } else if (option == "--mode") {
            if (value == "fast") {
                options.mode = ConsumerMode::Fast;
            } else if (value == "slow") {
                options.mode = ConsumerMode::Slow;
            } else {
                error = "Invalid --mode: expected fast or slow";
                return false;
            }
            haveMode = true;
        } else if (option == "--id") {
            if (!parseInteger(value, options.id) || options.id == 0) {
                error = "Invalid --id: expected a positive integer";
                return false;
            }
            haveId = true;
        } else if (option == "--delay-ms") {
            if (!parseInteger(value, options.delay_ms)) {
                error = "Invalid --delay-ms: expected a non-negative integer";
                return false;
            }
            haveDelay = true;
        } else {
            return unknownOption(option, error);
        }
    }

    if (!haveHost || !havePort || !haveMode || !haveId) {
        error = "Required options: --host --port --mode --id";
        return false;
    }
    if (options.mode == ConsumerMode::Slow) {
        if (!haveDelay) {
            error = "Required option for slow mode: --delay-ms";
            return false;
        }
        if (options.delay_ms == 0) {
            error = "Invalid --delay-ms for slow mode: expected a positive integer";
            return false;
        }
    }
    if (options.mode == ConsumerMode::Fast) {
        options.delay_ms = 0;
    }
    return true;
}

} // namespace eventstream
