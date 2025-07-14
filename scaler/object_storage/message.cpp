#include "scaler/object_storage/message.h"

namespace scaler {
namespace object_storage {

kj::Array<const capnp::word> ObjectRequestHeader::toBuffer() {
    capnp::MallocMessageBuilder returnMsg;
    auto respRoot = returnMsg.initRoot<::ObjectRequestHeader>();

    auto respRootObjectID = respRoot.initObjectID();
    respRootObjectID.setField0(objectID[0]);
    respRootObjectID.setField1(objectID[1]);
    respRootObjectID.setField2(objectID[2]);
    respRootObjectID.setField3(objectID[3]);

    respRoot.setPayloadLength(payloadLength);
    respRoot.setRequestID(requestID);
    respRoot.setRequestType(requestType);

    return capnp::messageToFlatArray(returnMsg);
}

kj::Array<const capnp::word> ObjectResponseHeader::toBuffer() {
    capnp::MallocMessageBuilder returnMsg;
    auto respRoot = returnMsg.initRoot<::ObjectResponseHeader>();

    auto respRootObjectID = respRoot.initObjectID();
    respRootObjectID.setField0(objectID[0]);
    respRootObjectID.setField1(objectID[1]);
    respRootObjectID.setField2(objectID[2]);
    respRootObjectID.setField3(objectID[3]);

    respRoot.setPayloadLength(payloadLength);
    respRoot.setResponseID(responseID);
    respRoot.setResponseType(responseType);

    return capnp::messageToFlatArray(returnMsg);
}

};  // namespace object_storage
};  // namespace scaler
