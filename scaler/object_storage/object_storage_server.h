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
    struct PendingRequest {
        std::shared_ptr<tcp::socket> socket;
        ObjectRequestHeader requestHeader;
    };

    struct PendingDuplicateRequest {
        std::shared_ptr<tcp::socket> socket;
        ObjectRequestHeader requestHeader;
    };

    using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
    using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

    boost::asio::io_context ioContext {1};

    int onServerReadyReader;
    int onServerReadyWriter;

    ObjectRegister objectRegister;

    // Some GET and DUPLICATE requests might be delayed if the referenced object isn't available yet.
    std::map<ObjectID, std::vector<PendingRequest>> pendingRequests;

    void initServerReadyFds();

    void setServerReadyFd();

    void closeServerReadyFds();

    awaitable<void> listener(tcp::endpoint endpoint);

    awaitable<void> processRequests(std::shared_ptr<tcp::socket> socket);

    awaitable<void> processSetRequest(std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& requestHeader);

    awaitable<void> processGetRequest(std::shared_ptr<tcp::socket> socket, const ObjectRequestHeader& requestHeader);

    awaitable<void> processDeleteRequest(std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& requestHeader);

    awaitable<void> processDuplicateRequest(std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& requestHeader);

    template <Message T>
    awaitable<T> readMessage(std::shared_ptr<tcp::socket> socket) {
        try {
            std::array<uint64_t, T::bufferSize() / CAPNP_WORD_SIZE> buffer;
            std::size_t n = co_await boost::asio::async_read(
                *socket, boost::asio::buffer(buffer.data(), T::bufferSize()), use_awaitable);

            // TODO: check the value of `n`
            co_return T::fromBuffer(buffer);
        } catch (boost::system::system_error& e) {
            // TODO: make this a log, since eof is not really an err.
            if (e.code() == boost::asio::error::eof) {
                std::cerr << "Remote end closed, nothing to read.\n";
            } else {
                std::cerr << "exception thrown, read error e.what() = " << e.what() << '\n';
            }
            throw e;
        } catch (std::exception& e) {
            // TODO: make this a log, capnp header corruption is an err.
            std::cerr << "exception thrown, message not a capnp e.what() = " << e.what() << '\n';
            throw e;
        }
    }

    template <Message T>
    boost::asio::awaitable<void> writeMessage(
        std::shared_ptr<tcp::socket> socket, T& message, std::span<const unsigned char> payload) {
        auto messageBuffer = message.toBuffer();

        std::array<boost::asio::const_buffer, 2> buffers {
            boost::asio::buffer(messageBuffer.asBytes().begin(), messageBuffer.asBytes().size()),
            boost::asio::buffer(payload),
        };

        try {
            // FIXME: all calls to async_write should be protected by an asio lock, as async_write is a "composed" asio
            // operation.
            // See https://www.boost.org/doc/libs/1_72_0/doc/html/boost_asio/reference/async_write/overload1.html.
            co_await boost::asio::async_write(*socket, buffers, use_awaitable);
        } catch (boost::system::system_error& e) {
            // TODO: Log support
            if (e.code() == boost::asio::error::broken_pipe) {
                std::cerr << "Remote end closed, nothing to write.\n";
                std::cerr << "This should never happen as the client is expected "
                          << "to get every and all response. Terminating now...\n";
                std::terminate();
            } else {
                std::cerr << "write error e.what() = " << e.what() << '\n';
            }
            throw e;
        }
    }

    awaitable<void> sendGetResponse(
        std::shared_ptr<tcp::socket> socket,
        const ObjectRequestHeader& requestHeader,
        std::shared_ptr<const ObjectPayload> objectPtr);

    awaitable<void> sendDuplicateResponse(
        std::shared_ptr<tcp::socket> socket, const ObjectRequestHeader& requestHeader);

    awaitable<void> optionallySendPendingRequests(
        const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr);
};

};  // namespace object_storage
};  // namespace scaler
