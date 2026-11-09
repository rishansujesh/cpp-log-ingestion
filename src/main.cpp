#include <fstream>
#include <iostream>
#include <log_ingest/engine.hpp>
#include <string>

int main(int argc, char **argv) {
    if (argc != 3 || (std::string(argv[1]) != "spsc" && std::string(argv[1]) != "locked")) {
        std::cerr << "Usage: log-ingest {spsc|locked} OUTPUT_FILE < input.log\n";
        return 2;
    }
    try {
        std::ofstream output(argv[2], std::ios::app | std::ios::binary);
        if (!output)
            throw std::runtime_error("cannot open output file");
        auto ingest = [&]<class Queue>() {
            log_ingest::Engine<Queue> engine(4096, log_ingest::line_sink(output),
                                             log_ingest::OverflowPolicy::block);
            std::string line;
            while (std::getline(std::cin, line)) {
                if (engine.submit(std::move(line)) != log_ingest::SubmitResult::accepted)
                    throw std::runtime_error("record rejected (maximum record size: 65536 bytes)");
            }
            if (std::cin.bad())
                throw std::runtime_error("input read failed");
            engine.close();
            output.flush();
            if (!output)
                throw std::runtime_error("output flush failed");
            auto stats = engine.stats();
            std::cerr << "accepted=" << stats.accepted << " written=" << stats.written << '\n';
        };
        if (std::string(argv[1]) == "spsc")
            ingest.template operator()<log_ingest::SpscQueue>();
        else
            ingest.template operator()<log_ingest::LockedQueue>();
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
