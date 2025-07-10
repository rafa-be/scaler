#pragma once

#include <unistd.h>

#include <algorithm>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/system/system_error.hpp>
#include <iostream>
#include <map>
#include <utility>

#include "protocol/object_storage.capnp.h"
#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/io_helper.h"

template <>
struct std::hash<scaler::object_storage::ObjectPayload> {
    std::size_t operator()(const scaler::object_storage::ObjectPayload& payload) const noexcept {
        return std::hash<std::string_view> {}({reinterpret_cast<const char*>(payload.data()), payload.size()});
    }
};

namespace scaler {
namespace object_storage {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

class ObjectStorageServer {
public:
    ObjectStorageServer() { this->initServerReadyFds(); }

    ~ObjectStorageServer() { this->closeServerReadyFds(); }

    void run(std::string name, std::string port) {
        try {
            boost::asio::io_context ioContext(1);
            tcp::resolver resolver(ioContext);
            auto res = resolver.resolve(name, port);

            boost::asio::signal_set signals(ioContext, SIGINT, SIGTERM);
            signals.async_wait([&](auto, auto) { ioContext.stop(); });

            co_spawn(ioContext, listener(res.begin()->endpoint()), detached);
            ioContext.run();
        } catch (std::exception& e) {
            std::cerr << "Exception: " << e.what() << std::endl;
            std::cerr << "Mostly something serious happen, inspect capnp header corruption" << std::endl;
        }
    }

    void waitUntilReady() {
        uint64_t value;
        ssize_t ret = read(this->onServerReadyReader, &value, sizeof(uint64_t));

        if (ret != sizeof(uint64_t)) {
            std::cerr << "read from onServerReadyReader failed: errno=" << errno << std::endl;
            std::terminate();
        }
    }

private:
    struct Meta {
        std::shared_ptr<tcp::socket> socket;
        ObjectRequestHeader requestHeader;
        ObjectResponseHeader responseHeader;
    };

    struct ObjectWithMeta {
        SharedObjectPayload object;
        std::vector<Meta> metaInfo;
    };

    using ObjectRequestType  = ::ObjectRequestHeader::ObjectRequestType;
    using ObjectResponseType = ::ObjectResponseHeader::ObjectResponseType;

    int onServerReadyReader;
    int onServerReadyWriter;

    std::map<ObjectID, ObjectWithMeta> objectIDToMeta;
    std::map<std::size_t, SharedObjectPayload> objectHashToObject;

    void initServerReadyFds() {
        int pipeFds[2];
        int ret = pipe(pipeFds);

        if (ret != 0) {
            std::cerr << "create on server ready FDs failed: errno=" << errno << std::endl;
            std::terminate();
        }

        this->onServerReadyReader = pipeFds[0];
        this->onServerReadyWriter = pipeFds[1];
    }

    void setServerReadyFd() {
        uint64_t value = 1;
        ssize_t ret    = write(this->onServerReadyWriter, &value, sizeof(uint64_t));

        if (ret != sizeof(uint64_t)) {
            std::cerr << "write to onServerReadyWriter failed: errno=" << errno << std::endl;
            std::terminate();
        }
    }

    void closeServerReadyFds() {
        std::array<int, 2> fds {this->onServerReadyReader, this->onServerReadyWriter};

        for (auto fd: fds) {
            if (close(fd) != 0) {
                std::cerr << "close failed: errno=" << errno << std::endl;
                std::terminate();
            }
        }
    }

    awaitable<void> listener(tcp::endpoint endpoint) {
        auto executor = co_await boost::asio::this_coro::executor;
        tcp::acceptor acceptor(executor, endpoint);

        setServerReadyFd();

        for (;;) {
            auto sharedSocket = std::make_shared<tcp::socket>(executor);
            co_await acceptor.async_accept(*sharedSocket, use_awaitable);
            setTCPNoDelay(*sharedSocket, true);

            co_spawn(executor, processRequest(std::move(sharedSocket)), detached);
        }
    }

    awaitable<void> processRequest(std::shared_ptr<tcp::socket> socket) {
        try {
            for (;;) {
                ObjectRequestHeader requestHeader;
                co_await readRequestHeader(*socket, requestHeader);

                ObjectPayload payload;
                co_await readRequestPayload(*socket, requestHeader, payload);

                ObjectResponseHeader responseHeader;
                bool nonBlockingRequest = updateRecord(requestHeader, responseHeader, std::move(payload));

                co_await optionallySendPendingRequests(requestHeader);

                if (!nonBlockingRequest) {
                    objectIDToMeta[requestHeader.objectID].metaInfo.emplace_back(socket, requestHeader, responseHeader);
                    continue;
                }

                auto payloadView = getMemoryViewForResponsePayload(responseHeader);

                co_await writeResponse(*socket, responseHeader, payloadView);
            }
        } catch (std::exception& e) {
            // TODO: Logging support
            // std::printf("process_request Exception: %s\n", e.what());
        }
    }

    bool updateRecord(
        const ObjectRequestHeader& requestHeader, ObjectResponseHeader& responseHeader, ObjectPayload payload) {
        responseHeader.objectID   = requestHeader.objectID;
        responseHeader.responseID = requestHeader.requestID;
        switch (requestHeader.reqType) {
            case ObjectRequestType::SET_OBJECT: {
                auto objectHash = std::hash<ObjectPayload> {}(payload);
                if (!objectHashToObject.contains(objectHash)) {
                    objectHashToObject[objectHash] = std::make_shared<ObjectPayload>(std::move(payload));
                }
                responseHeader.respType                       = ObjectResponseType::SET_O_K;
                objectIDToMeta[requestHeader.objectID].object = objectHashToObject[objectHash];

                break;
            }

            case ObjectRequestType::GET_OBJECT: {
                responseHeader.respType = ObjectResponseType::GET_O_K;
                if (objectIDToMeta[requestHeader.objectID].object) {
                    uint64_t objectSize = static_cast<uint64_t>(objectIDToMeta[requestHeader.objectID].object->size());
                    responseHeader.payloadLength = std::min(objectSize, requestHeader.payloadLength);
                } else
                    return false;
                break;
            }

            case ObjectRequestType::DELETE_OBJECT: {
                responseHeader.respType = objectIDToMeta[requestHeader.objectID].object ?
                                              ObjectResponseType::DEL_O_K :
                                              ObjectResponseType::DEL_NOT_EXISTS;
                auto sharedObject       = objectIDToMeta[requestHeader.objectID].object;
                objectIDToMeta.erase(requestHeader.objectID);
                if (sharedObject.use_count() == 2) {
                    objectHashToObject.erase(std::hash<ObjectPayload> {}(*sharedObject));
                }
                break;
            }
        }
        return true;
    }

    std::span<const unsigned char> getMemoryViewForResponsePayload(ObjectResponseHeader& header) {
        switch (header.respType) {
            case ObjectResponseType::GET_O_K:
                return {objectIDToMeta[header.objectID].object->data(), header.payloadLength};
            case ObjectResponseType::SET_O_K:
            case ObjectResponseType::DEL_O_K:
            case ObjectResponseType::DEL_NOT_EXISTS:
            default: break;
        }
        return {static_cast<const unsigned char*>(nullptr), 0};
    }

    awaitable<void> writeOnce(Meta meta) {
        if (meta.requestHeader.reqType == ObjectRequestType::GET_OBJECT) {
            uint64_t objectSize = static_cast<uint64_t>(objectIDToMeta[meta.responseHeader.objectID].object->size());
            meta.responseHeader.payloadLength = std::min(objectSize, meta.requestHeader.payloadLength);
        }

        auto payload_view = getMemoryViewForResponsePayload(meta.responseHeader);
        co_await writeResponse(*meta.socket, meta.responseHeader, payload_view);
    }

    awaitable<void> optionallySendPendingRequests(ObjectRequestHeader requestHeader) {
        if (requestHeader.reqType == ObjectRequestType::SET_OBJECT) {
            for (auto& curr_meta: objectIDToMeta[requestHeader.objectID].metaInfo) {
                try {
                    co_await writeOnce(std::move(curr_meta));
                } catch (boost::system::system_error& e) {
                    std::cerr << "Mostly because some connections disconnected accidentally.\n";
                }
            }
            objectIDToMeta[requestHeader.objectID].metaInfo = std::vector<Meta>();
        }
        co_return;
    }
};

};  // namespace object_storage
};  // namespace scaler
