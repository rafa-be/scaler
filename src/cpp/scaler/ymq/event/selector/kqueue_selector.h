#pragma once

#include <sys/event.h>
#include <sys/types.h>

#include <chrono>
#include <optional>
#include <vector>

#include "scaler/ymq/event/selector/selector.h"

namespace scaler {
namespace ymq {
namespace event {
namespace selector {

class KQueueSelector {
public:
    using Handle = uintptr_t;

    KQueueSelector();
    ~KQueueSelector();

    KQueueSelector(const KQueueSelector&)            = delete;
    KQueueSelector& operator=(const KQueueSelector&) = delete;
    KQueueSelector(KQueueSelector&&)                 = delete;
    KQueueSelector& operator=(KQueueSelector&&)      = delete;

    void add(Handle handle, EventType events);

    void remove(Handle Handle);

    std::vector<SelectorEvent<KQueueSelector>> select(std::optional<std::chrono::milliseconds> timeout = std::nullopt);

private:
    constexpr static const size_t _MAX_EVENTS = 1024;

    int _kq;

    void _kqueueCreate();

    void _setKEvent(int op, const struct kevent* event);
};

}  // namespace selector
}  // namespace event
}  // namespace ymq
}  // namespace scaler
