#pragma once

#include <concepts>

#include "scaler/utility/timestamp.h"
#include "scaler/ymq/event/event_type.h"
#include "scaler/ymq/event_manager.h"

namespace scaler {
namespace ymq {
namespace event {

template <typename T>
concept EventLoop = requires(T loop, typename T::Function function, typename T::TaskIdentifier taskIdent) {
    typename T::Function;
    typename T::TaskIdentifier;

    // Call this function during the next iteration of the event loop. Must be called from the event loop thread.
    { loop.executeSoon(std::move(function)) } -> std::same_as<void>;

    { loop.executeSoonThreadSafe(std::move(function)) } -> std::same_as<void>;

    // Plan the execution of a function in the future. Must be called from the event loop thread.
    { loop.executeAt(utility::Timestamp {}, std::move(function)) } -> std::same_as<typename T::TaskIdentifier>;

    // Cancel the execution of a function in the future. Must be called from the event loop thread.
    { loop.cancelExecution(taskIdent) } -> std::same_as<void>;

    // Register a file descriptor for events. Must be called from the event loop thread.
    { loop.addFdToLoop(int {}, EventType {}, std::move(EventManager {})) } -> std::same_as<void>;
    { loop.removeFdFromLoop(int {}) } -> std::same_as<void>;

    // Execute one iteration of the event loop, calling ready functions and I/O events.
    { loop.routine() } -> std::same_as<void>;
};

}  // namespace event
}  // namespace ymq
}  // namespace scaler
