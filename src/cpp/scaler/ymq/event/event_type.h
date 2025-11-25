#pragma once

namespace scaler {
namespace ymq {
namespace event {

// The types of I/O events. Can be combined with binary AND/OR operators.
enum EventType { None = 0x00, Read = 0x01, Write = 0x02, Close = 0x04, Error = 0x08 };

}  // namespace event
}  // namespace ymq
}  // namespace scaler
