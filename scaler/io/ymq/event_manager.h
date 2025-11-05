#pragma once

// C++
#ifdef _WIN32
#include <windows.h>
#endif  // _WIN32

#include <functional>

// First-party
#include "scaler/io/ymq/configuration.h"

namespace scaler {
namespace ymq {

class EventLoopThread;

#if defined(_WIN32)
class EventManager: public OVERLAPPED {
#else
class EventManager {
#endif
public:
    // User that registered them should have everything they need
    // In the future, we might add more onXX() methods, for now these are all we need.
    using OnEventCallback = std::function<void()>;

    OnEventCallback onRead;
    OnEventCallback onWrite;
    OnEventCallback onClose;
    OnEventCallback onError;

    EventManager()
    {
#ifdef _WIN32
        ZeroMemory(this, sizeof(*this));
#endif  // _WIN32
    };
};

}  // namespace ymq
}  // namespace scaler
