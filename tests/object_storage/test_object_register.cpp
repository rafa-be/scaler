#include <gtest/gtest.h>

#include "scaler/object_storage/object_register.h"

const scaler::object_storage::ObjectPayload payload {'H', 'e', 'l', 'l', 'o'};

TEST(ObjectRegisterTestSuite, TestSetObject) {
    scaler::object_storage::ObjectRegister objectRegister;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};

    EXPECT_FALSE(objectRegister.hasObject(objectID1));
    EXPECT_EQ(objectRegister.size(), 0);
    EXPECT_EQ(objectRegister.size_unique(), 0);

    objectRegister.setObject(objectID1, std::move(std::vector(payload)));

    EXPECT_TRUE(objectRegister.hasObject(objectID1));
    EXPECT_EQ(objectRegister.size(), 1);
    EXPECT_EQ(objectRegister.size_unique(), 1);

    scaler::object_storage::ObjectID objectID2 {3, 2, 1, 0};

    objectRegister.setObject(objectID2, std::move(std::vector(payload)));

    EXPECT_TRUE(objectRegister.hasObject(objectID2));
    EXPECT_EQ(objectRegister.size(), 2);
    EXPECT_EQ(objectRegister.size_unique(), 1);
}

TEST(ObjectRegisterTestSuite, TestGetObject) {
    scaler::object_storage::ObjectRegister objectRegister;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};

    auto payloadPtr = objectRegister.getObject(objectID1);

    EXPECT_EQ(payloadPtr, nullptr);  // not yet existing object

    objectRegister.setObject(objectID1, std::move(std::vector(payload)));

    payloadPtr = objectRegister.getObject(objectID1);

    EXPECT_EQ(*payloadPtr, payload);
}

TEST(ObjectRegisterTestSuite, TestDeleteObject) {
    scaler::object_storage::ObjectRegister objectRegister;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};

    objectRegister.setObject(objectID1, std::move(std::vector(payload)));

    bool deleted = objectRegister.deleteObject(objectID1);
    EXPECT_TRUE(deleted);

    EXPECT_FALSE(objectRegister.hasObject(objectID1));
    EXPECT_EQ(objectRegister.size(), 0);
    EXPECT_EQ(objectRegister.size_unique(), 0);

    deleted = objectRegister.deleteObject(objectID1);  // deleting unknown object
    EXPECT_FALSE(deleted);
}

TEST(ObjectRegisterTestSuite, TestDuplicateObject) {
    scaler::object_storage::ObjectRegister objectRegister;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};
    scaler::object_storage::ObjectID objectID2 {0, 1, 2, 4};

    // Cannot duplicate a non existing object.
    auto duplicatedObject = objectRegister.duplicateObject(objectID1, objectID2);
    EXPECT_EQ(duplicatedObject, nullptr);

    objectRegister.setObject(objectID1, std::move(std::vector(payload)));

    duplicatedObject = objectRegister.duplicateObject(objectID1, objectID2);
    EXPECT_NE(duplicatedObject, nullptr);
    EXPECT_EQ(*duplicatedObject, payload);

    // Deleting the first object does not remove the duplicated one.
    objectRegister.deleteObject(objectID1);
    EXPECT_TRUE(objectRegister.hasObject(objectID2));
    EXPECT_EQ(objectRegister.size(), 1);
    EXPECT_EQ(objectRegister.size_unique(), 1);
}

TEST(ObjectRegisterTestSuite, TestReferenceCountObject) {
    scaler::object_storage::ObjectRegister objectRegister;

    scaler::object_storage::ObjectID objectID1 {11, 0, 0, 0};
    objectRegister.setObject(objectID1, std::move(std::vector(payload)));

    scaler::object_storage::ObjectID objectID2 {12, 0, 0, 0};
    objectRegister.setObject(objectID2, std::move(std::vector(payload)));

    EXPECT_EQ(objectRegister.size(), 2);
    EXPECT_EQ(objectRegister.size_unique(), 1);

    auto payloadPtr1 = objectRegister.getObject(objectID1);
    auto payloadPtr2 = objectRegister.getObject(objectID2);

    EXPECT_EQ(payloadPtr1, payloadPtr2);  // should use the same memory location

    objectRegister.deleteObject(objectID1);

    EXPECT_EQ(objectRegister.size(), 1);
    EXPECT_EQ(objectRegister.size_unique(), 1);

    objectRegister.deleteObject(objectID2);

    EXPECT_EQ(objectRegister.size(), 0);
    EXPECT_EQ(objectRegister.size_unique(), 0);
}
