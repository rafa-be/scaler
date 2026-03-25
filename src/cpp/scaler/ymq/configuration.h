#pragma once

#include <chrono>
#include <cstddef>

namespace scaler {
namespace ymq {

constexpr size_t defaultClientMaxRetryTimes = 4;
constexpr std::chrono::milliseconds defaultClientInitRetryDelay {2000};

constexpr int serverListenBacklog = 1024;

// Maximum size of a single uv_write buffer. Some OS discourages large writes (e.g. Windows fails above 512 MB).
constexpr size_t maxWriteBufferSize = 256ULL * 1024ULL * 1024ULL;  // 256 MB

}  // namespace ymq
}  // namespace scaler
