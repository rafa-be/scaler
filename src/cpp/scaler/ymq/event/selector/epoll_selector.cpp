#include "scaler/ymq/event/selector/epoll_selector.h"

#include <string.h>
#include <sys/epoll.h>

#include <chrono>
#include <optional>
#include <vector>

#include "scaler/utility/error.h"
#include "scaler/ymq/event/event_type.h"
#include "scaler/ymq/event/selector/selector.h"

namespace scaler {
namespace ymq {
namespace event {
namespace selector {

EpollSelector::EpollSelector()
{
    _epollCreate();
}

EpollSelector::~EpollSelector()
{
    ::close(_epFd);
}

void EpollSelector::add(Handle handle, EventType events)
{
    ::epoll_event event {};

    event.data.fd = handle;

    event.events = EPOLLET;
    if (events & EventType::Read) {
        event.events |= EPOLLIN;
    }
    if (events & EventType::Write) {
        event.events |= EPOLLOUT;
    }
    if (events & EventType::Close) {
        event.events |= EPOLLRDHUP;
    }

    _epollCtl(EPOLL_CTL_ADD, handle, &event);
}

void EpollSelector::remove(Handle handle)
{
    _epollCtl(EPOLL_CTL_DEL, handle, nullptr);
}

std::vector<SelectorEvent<EpollSelector>> EpollSelector::select(std::optional<std::chrono::milliseconds> timeout)
{
    std::array<epoll_event, _MAX_EVENTS> events {};
    int timeoutMs = timeout ? static_cast<int>(timeout->count()) : -1;
    int n         = ::epoll_wait(_epFd, events.data(), _MAX_EVENTS, timeoutMs);

    if (n == -1) {
        switch (errno) {
            case EINTR:
                // the epoll thread is not expected to receive signals(?)
                // but occasionally does (e.g. sigwinch) and we shouldn't stop the thread in that case
                return {};
            case EBADF:
            case EFAULT:
            case EINVAL:
            default:
                unrecoverableError({
                    utility::Error::ErrorCode::CoreBug,
                    "Originated from",
                    "epoll_wait(2)",
                    "Errno is",
                    strerror(errno),
                    "_epFd",
                    _epFd,
                });
                break;
        }
    }

    std::vector<SelectorEvent<EpollSelector>> result;

    for (const auto& event: std::span(events.data(), n)) {
        Handle handle = event.data.fd;

        EventType eventTypes = EventType::None;

        if (event.events & (EPOLLHUP | EPOLLRDHUP)) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Close);
        }
        if (event.events & EPOLLERR) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Error);
        }
        if (event.events & EPOLLIN) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Read);
        }
        if (event.events & EPOLLOUT) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Write);
        }

        result.emplace_back(handle, eventTypes);
    }

    return result;
}

void EpollSelector::_epollCreate()
{
    _epFd = ::epoll_create1(0);
    if (_epFd == -1) {
        switch (errno) {
            case ENFILE:
            case ENODEV:
            case ENOMEM:
            case EMFILE:
                unrecoverableError({
                    utility::Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "epoll_create1(2)",
                    "Errno is",
                    strerror(errno),
                });
                break;

            case EINVAL:
            default:
                unrecoverableError({
                    utility::Error::ErrorCode::CoreBug,
                    "Originated from",
                    "epoll_create1(2)",
                    "Errno is",
                    strerror(errno),
                });
                break;
        }
    }
}

void EpollSelector::_epollCtl(int op, int fd, epoll_event* event)
{
    int res = ::epoll_ctl(_epFd, op, fd, event);
    if (res == 0) {
        return;
    }

    switch (errno) {
        case ENOMEM:
        case ENOSPC:
            unrecoverableError({
                utility::Error::ErrorCode::ConfigurationError,
                "Originated from",
                "epoll_ctl(2)",
                "Errno is",
                strerror(errno),
                "epFd",
                _epFd,
                "fd",
                fd,
            });
            break;

        case EBADF:
        case EPERM:
        case EINVAL:
        case ELOOP:
        case ENOENT:
        default:
            unrecoverableError({
                utility::Error::ErrorCode::CoreBug,
                "Originated from",
                "epoll_ctl(2)",
                "Errno is",
                strerror(errno),
                "epFd",
                _epFd,
                "fd",
                fd,
            });
            break;
    }
}

static_assert(Selector<EpollSelector>);

}  // namespace selector
}  // namespace event
}  // namespace ymq
}  // namespace scaler
