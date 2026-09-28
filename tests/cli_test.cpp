#include <eventstream/cli.hpp>

#include <iostream>
#include <string>
#include <vector>

namespace {

void makeArgv(std::vector<std::string>& arguments,
              std::vector<char*>& argv) {
    argv.clear();
    argv.reserve(arguments.size());
    for (auto& argument : arguments) {
        argv.push_back(argument.data());
    }
}

bool expectProducerFailure(std::vector<std::string> arguments,
                           const char* caseName) {
    std::vector<char*> argv;
    makeArgv(arguments, argv);
    eventstream::ProducerOptions options;
    std::string error;
    const bool parsed = eventstream::parseProducerOptions(
        static_cast<int>(argv.size()), argv.data(), options, error);
    if (parsed || error.empty()) {
        std::cerr << "Producer " << caseName
                  << " failed: expected rejection with an error\n";
        return false;
    }
    return true;
}

bool expectServerFailure(std::vector<std::string> arguments,
                         const char* caseName) {
    std::vector<char*> argv;
    makeArgv(arguments, argv);
    eventstream::ServerOptions options;
    std::string error;
    const bool parsed = eventstream::parseServerOptions(
        static_cast<int>(argv.size()), argv.data(), options, error);
    if (parsed || error.empty()) {
        std::cerr << "Server " << caseName
                  << " failed: expected rejection with an error\n";
        return false;
    }
    return true;
}

bool expectConsumerFailure(std::vector<std::string> arguments,
                           const char* caseName) {
    std::vector<char*> argv;
    makeArgv(arguments, argv);
    eventstream::ConsumerOptions options;
    std::string error;
    const bool parsed = eventstream::parseConsumerOptions(
        static_cast<int>(argv.size()), argv.data(), options, error);
    if (parsed || error.empty()) {
        std::cerr << "Consumer " << caseName
                  << " failed: expected rejection with an error\n";
        return false;
    }
    return true;
}

bool testProducer() {
    std::vector<std::string> arguments{
        "esl_producer", "--host", "127.0.0.1", "--port", "9000",
        "--fps", "30", "--frames", "900"};
    std::vector<char*> argv;
    makeArgv(arguments, argv);
    eventstream::ProducerOptions options;
    std::string error;
    if (!eventstream::parseProducerOptions(static_cast<int>(argv.size()),
                                           argv.data(), options, error) ||
        !error.empty() || options.host != "127.0.0.1" || options.port != 9000 ||
        options.fps != 30 || options.frames != 900) {
        std::cerr << "Producer valid input failed: " << error << '\n';
        return false;
    }

    if (!expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "9000",
             "--fps", "30"},
            "missing required option") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "0",
             "--fps", "30", "--frames", "900"},
            "port zero") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "65536",
             "--fps", "30", "--frames", "900"},
            "port above range") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "abc",
             "--fps", "30", "--frames", "900"},
            "non-numeric port") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "9000",
             "--fps", "0", "--frames", "900"},
            "fps zero") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "9000",
             "--fps", "abc", "--frames", "900"},
            "non-numeric fps") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "9000",
             "--fps", "30", "--frames", "0"},
            "frames zero") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "9000",
             "--fps", "30", "--frames", "abc"},
            "non-numeric frames") ||
        !expectProducerFailure(
            {"esl_producer", "--banana", "123"}, "unknown option") ||
        !expectProducerFailure(
            {"esl_producer", "--host", "127.0.0.1", "--port", "9000",
             "--fps"},
            "missing option value")) {
        return false;
    }

    std::cout << "Producer CLI Tests passed\n";
    return true;
}

bool testServer() {
    std::vector<std::string> arguments{
        "esl_server", "--producer-port", "9000", "--consumer-port", "9001",
        "--expected-consumers", "10"};
    std::vector<char*> argv;
    makeArgv(arguments, argv);
    eventstream::ServerOptions options;
    std::string error;
    if (!eventstream::parseServerOptions(static_cast<int>(argv.size()),
                                         argv.data(), options, error) ||
        !error.empty() || options.producer_port != 9000 ||
        options.consumer_port != 9001 || options.expected_consumers != 10) {
        std::cerr << "Server valid input failed: " << error << '\n';
        return false;
    }

    if (!expectServerFailure(
            {"esl_server", "--producer-port", "0", "--consumer-port", "9001",
             "--expected-consumers", "10"},
            "producer port zero") ||
        !expectServerFailure(
            {"esl_server", "--producer-port", "65536", "--consumer-port", "9001",
             "--expected-consumers", "10"},
            "producer port above range") ||
        !expectServerFailure(
            {"esl_server", "--producer-port", "abc", "--consumer-port", "9001",
             "--expected-consumers", "10"},
            "non-numeric producer port") ||
        !expectServerFailure(
            {"esl_server", "--producer-port", "9000", "--consumer-port", "0",
             "--expected-consumers", "10"},
            "consumer port zero") ||
        !expectServerFailure(
            {"esl_server", "--producer-port", "9000", "--consumer-port", "65536",
             "--expected-consumers", "10"},
            "consumer port above range") ||
        !expectServerFailure(
            {"esl_server", "--producer-port", "9000", "--consumer-port", "abc",
             "--expected-consumers", "10"},
            "non-numeric consumer port") ||
        !expectServerFailure(
            {"esl_server", "--producer-port", "9000", "--consumer-port", "9001",
             "--expected-consumers", "0"},
            "zero expected consumers") ||
        !expectServerFailure(
            {"esl_server", "--producer-port", "9000", "--expected-consumers", "10"},
            "missing required option")) {
        return false;
    }

    std::cout << "Server CLI Tests passed\n";
    return true;
}

bool testConsumer() {
    std::vector<std::string> fastArguments{
        "esl_consumer", "--host", "127.0.0.1", "--port", "9001",
        "--mode", "fast", "--id", "1"};
    std::vector<char*> fastArgv;
    makeArgv(fastArguments, fastArgv);
    eventstream::ConsumerOptions fastOptions;
    std::string error;
    if (!eventstream::parseConsumerOptions(static_cast<int>(fastArgv.size()),
                                           fastArgv.data(), fastOptions, error) ||
        !error.empty() || fastOptions.host != "127.0.0.1" ||
        fastOptions.port != 9001 ||
        fastOptions.mode != eventstream::ConsumerMode::Fast ||
        fastOptions.id != 1 || fastOptions.delay_ms != 0) {
        std::cerr << "Consumer FAST valid input failed: " << error << '\n';
        return false;
    }

    std::vector<std::string> slowArguments{
        "esl_consumer", "--host", "127.0.0.1", "--port", "9001",
        "--mode", "slow", "--delay-ms", "100", "--id", "10"};
    std::vector<char*> slowArgv;
    makeArgv(slowArguments, slowArgv);
    eventstream::ConsumerOptions slowOptions;
    error.clear();
    if (!eventstream::parseConsumerOptions(static_cast<int>(slowArgv.size()),
                                           slowArgv.data(), slowOptions, error) ||
        !error.empty() || slowOptions.mode != eventstream::ConsumerMode::Slow ||
        slowOptions.id != 10 || slowOptions.delay_ms != 100) {
        std::cerr << "Consumer SLOW valid input failed: " << error << '\n';
        return false;
    }

    if (!expectConsumerFailure(
            {"esl_consumer", "--host", "127.0.0.1", "--port", "9001",
             "--mode", "turtle", "--id", "1"},
            "invalid mode") ||
        !expectConsumerFailure(
            {"esl_consumer", "--host", "127.0.0.1", "--port", "9001",
             "--mode", "slow", "--id", "10"},
            "slow mode missing delay") ||
        !expectConsumerFailure(
            {"esl_consumer", "--host", "127.0.0.1", "--port", "9001",
             "--mode", "slow", "--delay-ms", "0", "--id", "10"},
            "slow mode zero delay") ||
        !expectConsumerFailure(
            {"esl_consumer", "--host", "127.0.0.1", "--port", "9001",
             "--mode", "fast", "--id", "0"},
            "zero id")) {
        return false;
    }

    std::vector<std::string> fastWithDelayArguments{
        "esl_consumer", "--host", "127.0.0.1", "--port", "9001",
        "--mode", "fast", "--delay-ms", "100", "--id", "1"};
    std::vector<char*> fastWithDelayArgv;
    makeArgv(fastWithDelayArguments, fastWithDelayArgv);
    eventstream::ConsumerOptions fastWithDelayOptions;
    error.clear();
    if (!eventstream::parseConsumerOptions(
            static_cast<int>(fastWithDelayArgv.size()), fastWithDelayArgv.data(),
            fastWithDelayOptions, error) ||
        !error.empty() || fastWithDelayOptions.delay_ms != 0) {
        std::cerr << "Consumer FAST delay normalization failed: " << error
                  << '\n';
        return false;
    }

    std::cout << "Consumer CLI Tests passed\n";
    return true;
}

} // namespace

int main() {
    if (!testProducer() || !testServer() || !testConsumer()) {
        return 1;
    }
    std::cout << "All CLI tests passed\n";
    return 0;
}
