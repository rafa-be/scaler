#pragma once

#include <array>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <vector>

#include "protocol/object_storage.capnp.h"

namespace scaler {
namespace object_storage {

using ObjectID            = std::array<uint64_t, 4>;
using ObjectPayload       = std::vector<unsigned char>;
using SharedObjectPayload = std::shared_ptr<ObjectPayload>;

struct ObjectRequestHeader {
    ObjectID objectID;
    uint64_t payloadLength;
    uint64_t requestID;
    ::ObjectRequestHeader::ObjectRequestType reqType;

    ObjectRequestHeader(): objectID {}, payloadLength {}, requestID {}, reqType {} {}
};

struct ObjectResponseHeader {
    ObjectID objectID;
    uint64_t payloadLength;
    uint64_t responseID;
    ::ObjectResponseHeader::ObjectResponseType respType;

    ObjectResponseHeader(): objectID {}, payloadLength {}, responseID {}, respType {} {}
};

};  // namespace object_storage
};  // namespace scaler
