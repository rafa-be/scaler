#pragma once

#include <chrono>
#include <optional>
#include <vector>

#include "scaler/ymq/event/event_type.h"

namespace scaler {
namespace ymq {
namespace event {
namespace selector {

template <typename T>
struct SelectorEvent {
    T::Handle handle;
    EventType events;
};

// An event selector monitors multiple handles (i.e. file descriptors, sockets ...) for events.
template <typename T>
concept Selector = requires(T selector, typename T::Handle handle) {
    typename T::Handle;

    // Start monitoring the handle for the selected I/O events, with a pointer to an user data-structure.
    //
    // Will fail if the handle is already registered.
    { selector.add(handle, EventType {}) } -> std::same_as<void>;

    // Stop monitoring an handle for any I/O events.
    { selector.remove(handle) } -> std::same_as<void>;

    // Block until it collects events on the registered handle.
    //
    // If multiple events occurred on the same handle, these must be merged into a single entry.
    { selector.select(std::chrono::milliseconds {}) } -> std::same_as<std::vector<SelectorEvent<T>>>;
    { selector.select() } -> std::same_as<std::vector<SelectorEvent<T>>>;
};

}  // namespace selector
}  // namespace event
}  // namespace ymq
}  // namespace scaler
