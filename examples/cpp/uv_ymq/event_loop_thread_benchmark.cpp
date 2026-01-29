// Benchmark comparing EventLoopThread vs LockFreeEventLoopThread
//
// This benchmark measures:
// 1. Latency: Time from enqueue to execution
// 2. Throughput: Number of callbacks processed per second

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <latch>
#include <vector>

#include "scaler/uv_ymq/event_loop_thread.h"
#include "scaler/uv_ymq/lock_free_event_loop_thread.h"

using namespace std::chrono;

constexpr size_t nTasks = 1000000;

struct Statistics {
    double mean;
    double min;
    double max;
    double stdev;

    static Statistics calculate(std::vector<double>& values)
    {
        if (values.empty()) {
            return {0, 0, 0, 0};
        }

        std::sort(values.begin(), values.end());

        double sum = 0;
        for (double v: values) {
            sum += v;
        }

        double mean = sum / values.size();

        double variance = 0;
        for (double v: values) {
            double diff = v - mean;
            variance += diff * diff;
        }
        variance /= values.size();

        Statistics stats;
        stats.mean  = mean;
        stats.min   = values.front();
        stats.max   = values.back();
        stats.stdev = std::sqrt(variance);

        return stats;
    }

    void print(const std::string& name) const
    {
        std::cout << name << ":\n";
        std::cout << "  Mean:  " << std::fixed << std::setprecision(2) << mean << " μs\n";
        std::cout << "  Min:   " << std::fixed << std::setprecision(2) << min << " μs\n";
        std::cout << "  Max:   " << std::fixed << std::setprecision(2) << max << " μs\n";
        std::cout << "  Stdev: " << std::fixed << std::setprecision(2) << stdev << " μs\n";
    }
};

template <typename ThreadType>
void benchmarkLatency(const std::string& name)
{
    std::cout << "\n=== " << name << " Latency Benchmark ===\n";

    ThreadType thread;
    std::vector<double> latencies;
    latencies.reserve(nTasks);

    std::latch done(nTasks);

    for (size_t i = 0; i < nTasks; ++i) {
        auto start = high_resolution_clock::now();

        thread.executeThreadSafe([start, &latencies, &done]() {
            auto end     = high_resolution_clock::now();
            auto latency = duration_cast<nanoseconds>(end - start).count() / 1000.0;  // Convert to microseconds
            latencies.push_back(latency);
            done.count_down();
        });
    }

    done.wait();

    Statistics stats = Statistics::calculate(latencies);
    stats.print(name + " Latency");
}

template <typename ThreadType>
void benchmarkThroughput(const std::string& name)
{
    std::cout << "\n=== " << name << " Throughput Benchmark ===\n";

    ThreadType thread;
    std::atomic<size_t> counter {0};
    std::latch done(1);

    auto start = high_resolution_clock::now();

    for (size_t i = 0; i < nTasks; ++i) {
        thread.executeThreadSafe([&counter, &done, i]() {
            counter.fetch_add(1, std::memory_order_relaxed);
            if (i == nTasks - 1) {
                done.count_down();
            }
        });
    }

    done.wait();
    auto end = high_resolution_clock::now();

    auto duration_ms  = duration_cast<milliseconds>(end - start).count();
    double throughput = (nTasks * 1000.0) / duration_ms;  // callbacks per second

    std::cout << name << " Throughput:\n";
    std::cout << "  Total time: " << duration_ms << " ms\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(0) << throughput << " callbacks/sec\n";
}

int main()
{
    // Latency benchmarks
    benchmarkLatency<scaler::uv_ymq::EventLoopThread>("EventLoopThread");
    benchmarkLatency<scaler::uv_ymq::LockFreeEventLoopThread>("LockFreeEventLoopThread");

    // Throughput benchmarks
    benchmarkThroughput<scaler::uv_ymq::EventLoopThread>("EventLoopThread");
    benchmarkThroughput<scaler::uv_ymq::LockFreeEventLoopThread>("LockFreeEventLoopThread");

    return 0;
}
