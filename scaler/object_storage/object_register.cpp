#include "scaler/object_storage/object_register.h"

#include <algorithm>

template <>
struct std::hash<scaler::object_storage::ObjectPayload> {
    std::size_t operator()(const scaler::object_storage::ObjectPayload& payload) const noexcept {
        return std::hash<std::string_view> {}({reinterpret_cast<const char*>(payload.data()), payload.size()});
    }
};

namespace scaler {
namespace object_storage {

ObjectRegister::ObjectRegister() {}

std::shared_ptr<const ObjectPayload> ObjectRegister::setObject(
    const ObjectID& objectID, ObjectPayload&& payload) noexcept {
    if (hasObject(objectID)) {
        // Overriding object: delete old first
        deleteObject(objectID);
    }

    ObjectHash hash = std::hash<ObjectPayload> {}(payload);

    objectIDToHash[objectID] = hash;

    auto objectIt = hashToObject.find(hash);

    if (objectIt == hashToObject.end()) {
        // New object payload
        objectIt = hashToObject
                       .emplace(
                           hash,
                           RegisteredObject {
                               .useCount = 1,
                               .payload  = std::make_shared<const ObjectPayload>(std::move(payload)),
                           })
                       .first;
    } else {
        // Known object payload
        ++(objectIt->second.useCount);
    }

    return objectIt->second.payload;
}

std::shared_ptr<const ObjectPayload> ObjectRegister::getObject(const ObjectID& objectID) const noexcept {
    auto hashIt = objectIDToHash.find(objectID);

    if (hashIt == objectIDToHash.end()) {
        return SharedObjectPayload(nullptr);
    }

    return hashToObject.at(hashIt->second).payload;
}

bool ObjectRegister::deleteObject(const ObjectID& objectID) noexcept {
    auto hashIt = objectIDToHash.find(objectID);

    if (hashIt == objectIDToHash.end()) {
        return false;
    }

    const ObjectHash& hash = hashIt->second;

    auto objectIt = hashToObject.find(hash);

    --objectIt->second.useCount;
    if (objectIt->second.useCount < 1) {
        hashToObject.erase(objectIt);
    }

    objectIDToHash.erase(hashIt);

    return true;
}

bool ObjectRegister::hasObject(const ObjectID& objectID) const noexcept {
    return objectIDToHash.contains(objectID);
}

size_t ObjectRegister::size() const noexcept {
    return objectIDToHash.size();
}

size_t ObjectRegister::size_unique() const noexcept {
    return hashToObject.size();
}

};  // namespace object_storage
};  // namespace scaler