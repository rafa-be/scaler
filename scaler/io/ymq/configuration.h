#pragma once

#include <expected>
#include <functional>
#include <memory>
#include <string>

// Because the devil says "You shall live with errors".
// ^-- The linker complains when the file is not here.
#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/move_only_function.h"

// Because the devil casts spells in plain English.
#ifdef _WIN32
#undef SendMessageCallback
#define __PRETTY_FUNCTION__ __FUNCSIG__
#endif  // _WIN32

namespace scaler {
namespace ymq {

class EpollContext;
class IocpContext;
class KqueueContext;
struct Message;
class IOSocket;

constexpr const uint64_t IOCP_SOCKET_CLOSED = 4;

struct Configuration {
#ifdef __linux__
    using PollingContext = EpollContext;
#endif  // __linux__
#ifdef _WIN32
    using PollingContext = IocpContext;
#endif  // _WIN32
#ifdef __APPLE__
    using PollingContext = KqueueContext;
#endif  // __APPLE__

    using IOSocketIdentity                = std::string;
    using SendMessageCallback             = MoveOnlyFunction<void(std::expected<void, Error>)>;
    using RecvMessageCallback             = MoveOnlyFunction<void(std::pair<Message, Error>)>;
    using ConnectReturnCallback           = MoveOnlyFunction<void(std::expected<void, Error>)>;
    using BindReturnCallback              = MoveOnlyFunction<void(std::expected<void, Error>)>;
    using CreateIOSocketCallback          = MoveOnlyFunction<void(std::shared_ptr<IOSocket>)>;
    using TimedQueueCallback              = MoveOnlyFunction<void()>;
    using ExecutionFunction               = MoveOnlyFunction<void()>;
    using ExecutionCancellationIdentifier = size_t;
};

}  // namespace ymq
}  // namespace scaler
