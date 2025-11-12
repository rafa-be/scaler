#pragma once

#include <chrono>
#include <concepts>
#include <optional>
#include <utility>
#include <vector>

#ifdef __linux__
#include <sys/epoll.h>
#endif  // __linux__

#ifdef __APPLE__

#include <sys/event.h>
#include <sys/types.h>

#endif  // __APPLE__

#include "scaler/io/ymq/error.h"

namespace scaler {
namespace ymq {

// The types of I/O events. Can be combined with binary AND/OR operators.
enum EventSelectorType { None = 0x00, Read = 0x01, Write = 0x02, Close = 0x04, Error = 0x08 };

template <typename T>
struct EventSelectorEvent {
    T identifier;
    EventSelectorType types;
};

// An event selector monitors multiple identifiers (i.e. file descriptors, sockets ...) for events.
template <typename T>
concept EventSelector = requires(T selector, typename T::Identifier ident) {
    typename T::Identifier;

    // Start monitoring the identifier for the selected I/O events, with a pointer to an user data-structure.
    //
    // Will fail if the identifier is already registered.
    { selector.add(ident, EventSelectorType {}) } -> std::same_as<void>;

    // Stop monitoring an identifier for any I/O events.
    { selector.remove(ident) } -> std::same_as<void>;

    // Block until it collects events on the registered identifiers.
    //
    // If multiple even occurred on the same identifier, these must be merged into a single entry.
    {
        selector.select(std::chrono::milliseconds {})
    } -> std::same_as<std::vector<EventSelectorEvent<typename T::Identifier>>>;
    { selector.select() } -> std::same_as<std::vector<EventSelectorEvent<typename T::Identifier>>>;
};

#ifdef __linux__

class EpollSelector {
public:
    using Identifier = int;

    EpollSelector();
    ~EpollSelector();

    EpollSelector(const EpollSelector&)            = delete;
    EpollSelector& operator=(const EpollSelector&) = delete;
    EpollSelector(EpollSelector&&)                 = delete;
    EpollSelector& operator=(EpollSelector&&)      = delete;

    void add(Identifier ident, EventSelectorType events);

    void remove(Identifier ident);

    std::vector<EventSelectorEvent<Identifier>> select(std::optional<std::chrono::milliseconds> timeout = std::nullopt);

private:
    constexpr static const size_t _MAX_EVENTS = 1024;

    int _epFd;
    int _interruptFd;

    void _epollCreate();

    void _epollCtl(int op, int fd, epoll_event* event);

    void _registerInterruptFd();

    void _resetInterruptFd();
};

#endif  // __linux__

#ifdef __APPLE__

class KQueueSelector {
public:
    using Identifier = uintptr_t;

    KQueueSelector();
    ~KQueueSelector();

    KQueueSelector(const KQueueSelector&)            = delete;
    KQueueSelector& operator=(const KQueueSelector&) = delete;
    KQueueSelector(KQueueSelector&&)                 = delete;
    KQueueSelector& operator=(KQueueSelector&&)      = delete;

    void add(Identifier ident, EventSelectorType events);

    void remove(Identifier ident);

    std::vector<EventSelectorEvent<Identifier>> select(std::optional<std::chrono::milliseconds> timeout = std::nullopt);

private:
    constexpr static const size_t _MAX_EVENTS      = 1024;
    constexpr static const int _INTERRUPT_EVENT_ID = 0;

    int _kq;

    void _kqueueCreate();

    void _registerInterruptEvent();

    void _setKEvent(int op, const struct kevent* event);
};

#endif  // __APPLE__

}  // namespace ymq
}  // namespace scaler
