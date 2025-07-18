#pragma once

#include <unistd.h>

#include <algorithm>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/system_error.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <utility>

#include "protocol/object_storage.capnp.h"
#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/io_helper.h"
#include "scaler/object_storage/object_register.h"

namespace scaler {
namespace object_storage {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

class ObjectStorageServer {
public:
    ObjectStorageServer();

    ~ObjectStorageServer();

    void run(std::string name, std::string port);

    void waitUntilReady();

    void shutdown();

private:
    struct PendingGetRequest {
        std::shared_ptr<tcp::socket> socket;
        ObjectRequestHeader requestHeader;
    };

    using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
    using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

    boost::asio::io_context ioContext {1};

    int onServerReadyReader;
    int onServerReadyWriter;

    ObjectRegister objectRegister;

    std::map<ObjectID, std::vector<PendingGetRequest>> pendingGetRequests;

    void initServerReadyFds();

    void setServerReadyFd();

    void closeServerReadyFds();

    awaitable<void> listener(tcp::endpoint endpoint);

    awaitable<void> processRequests(std::shared_ptr<tcp::socket> socket);

    awaitable<void> processSetRequest(std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& requestHeader);

    awaitable<void> processGetRequest(std::shared_ptr<tcp::socket> socket, const ObjectRequestHeader& requestHeader);

    awaitable<void> processDeleteRequest(std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& requestHeader);

    awaitable<void> readRequestHeader(std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& header);

    boost::asio::awaitable<void> writeResponse(
        std::shared_ptr<tcp::socket> socket, ObjectResponseHeader& header, std::span<const unsigned char> payload);

    awaitable<void> sendGetResponse(
        std::shared_ptr<tcp::socket> socket,
        const ObjectRequestHeader& requestHeader,
        std::shared_ptr<const ObjectPayload> objectPtr);

    awaitable<void> optionallySendPendingRequests(
        const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr);
};

};  // namespace object_storage
};  // namespace scaler
