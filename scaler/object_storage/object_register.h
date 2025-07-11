#pragma once

#include <map>

#include "scaler/object_storage/defs.h"

namespace scaler {
namespace object_storage {

class ObjectRegister {
public:
    ObjectRegister();

    bool hasObject(const ObjectID& objectID) const noexcept;

    void setObject(const ObjectID& objectID, ObjectPayload&& payload) noexcept;

    // Returns `nullptr` if the object does not exist.
    std::shared_ptr<const ObjectPayload> getObject(const ObjectID& objectID) const noexcept;

    // Returns `true` if the deleted object existed, otherwise returns `false`.
    bool deleteObject(const ObjectID& objectID) noexcept;

private:
    using ObjectHash = std::size_t;

    struct RegisteredObject {
        size_t useCount;
        std::shared_ptr<const ObjectPayload> payload;
    };

    std::map<ObjectID, ObjectHash> objectIDToHash;
    std::map<ObjectHash, RegisteredObject> hashToObject;
};

};  // namespace object_storage
};  // namespace scaler
