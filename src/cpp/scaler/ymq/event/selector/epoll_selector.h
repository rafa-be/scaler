#pragma once

#include <sys/epoll.h>

#include <chrono>
#include <optional>
#include <vector>

#include "scaler/ymq/event/selector/selector.h"

namespace scaler {
namespace ymq {
namespace event {
namespace selector {

class EpollSelector {
public:
    using Handle = int;

    EpollSelector();
    ~EpollSelector();

    EpollSelector(const EpollSelector&)            = delete;
    EpollSelector& operator=(const EpollSelector&) = delete;
    EpollSelector(EpollSelector&&)                 = delete;
    EpollSelector& operator=(EpollSelector&&)      = delete;

    void add(Handle handle, EventType events);

    void remove(Handle handle);

    std::vector<SelectorEvent<EpollSelector>> select(std::optional<std::chrono::milliseconds> timeout = std::nullopt);

private:
    constexpr static const size_t _MAX_EVENTS = 1024;

    int _epFd;
    int _interruptFd;

    void _epollCreate();

    void _epollCtl(int op, int fd, epoll_event* event);
};

}  // namespace selector
}  // namespace event
}  // namespace ymq
}  // namespace scaler
