#pragma once

#include <capnp/message.h>
#include <capnp/serialize.h>

#include "protocol/object_storage.capnp.h"
#include "scaler/object_storage/constants.h"
#include "scaler/object_storage/defs.h"

namespace scaler {
namespace object_storage {

struct ObjectRequestHeader {
    ObjectID objectID;
    uint64_t payloadLength;
    uint64_t requestID;
    scaler::protocol::ObjectRequestHeader::ObjectRequestType requestType;

    kj::Array<const capnp::word> toBuffer() const;

    template <typename Buffer>
    static ObjectRequestHeader fromBuffer(const Buffer& buffer) {
        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buffer.data(), CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE));

        auto requestRoot  = reader.getRoot<scaler::protocol::ObjectRequestHeader>();
        auto objectIDRoot = requestRoot.getObjectID();

        return ObjectRequestHeader {
            .objectID =
                {
                    objectIDRoot.getField0(),
                    objectIDRoot.getField1(),
                    objectIDRoot.getField2(),
                    objectIDRoot.getField3(),
                },
            .payloadLength = requestRoot.getPayloadLength(),
            .requestID     = requestRoot.getRequestID(),
            .requestType   = requestRoot.getRequestType(),
        };
    }
};

struct ObjectResponseHeader {
    ObjectID objectID;
    uint64_t payloadLength;
    uint64_t responseID;
    scaler::protocol::ObjectResponseHeader::ObjectResponseType responseType;

    kj::Array<const capnp::word> toBuffer() const;

    template <typename Buffer>
    static ObjectResponseHeader fromBuffer(const Buffer& buffer) {
        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buffer.data(), CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE));

        auto responseRoot = reader.getRoot<scaler::protocol::ObjectResponseHeader>();
        auto objectIDRoot = responseRoot.getObjectID();

        return ObjectResponseHeader {
            .objectID =
                {
                    objectIDRoot.getField0(),
                    objectIDRoot.getField1(),
                    objectIDRoot.getField2(),
                    objectIDRoot.getField3(),
                },
            .payloadLength = responseRoot.getPayloadLength(),
            .responseID    = responseRoot.getResponseID(),
            .responseType  = responseRoot.getResponseType(),
        };
    }
};

};  // namespace object_storage
};  // namespace scaler
