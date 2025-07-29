#pragma once

#include <array>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <vector>

namespace scaler {
namespace object_storage {

using ObjectPayload       = std::vector<unsigned char>;
using SharedObjectPayload = std::shared_ptr<ObjectPayload>;

};  // namespace object_storage
};  // namespace scaler
