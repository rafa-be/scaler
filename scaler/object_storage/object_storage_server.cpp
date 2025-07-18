#include "scaler/object_storage/object_storage_server.h"

namespace scaler {
namespace object_storage {

ObjectStorageServer::ObjectStorageServer() {
    initServerReadyFds();
}

ObjectStorageServer::~ObjectStorageServer() {
    shutdown();
    closeServerReadyFds();
}

void ObjectStorageServer::run(std::string name, std::string port) {
    try {
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

void ObjectStorageServer::waitUntilReady() {
    uint64_t value;
    ssize_t ret = read(onServerReadyReader, &value, sizeof(uint64_t));

    if (ret != sizeof(uint64_t)) {
        std::cerr << "read from onServerReadyReader failed: errno=" << errno << std::endl;
        std::terminate();
    }
}

void ObjectStorageServer::shutdown() {
    ioContext.stop();
}

void ObjectStorageServer::initServerReadyFds() {
    int pipeFds[2];
    int ret = pipe(pipeFds);

    if (ret != 0) {
        std::cerr << "create on server ready FDs failed: errno=" << errno << std::endl;
        std::terminate();
    }

    onServerReadyReader = pipeFds[0];
    onServerReadyWriter = pipeFds[1];
}

void ObjectStorageServer::setServerReadyFd() {
    uint64_t value = 1;
    ssize_t ret    = write(onServerReadyWriter, &value, sizeof(uint64_t));

    if (ret != sizeof(uint64_t)) {
        std::cerr << "write to onServerReadyWriter failed: errno=" << errno << std::endl;
        std::terminate();
    }
}

void ObjectStorageServer::closeServerReadyFds() {
    std::array<int, 2> fds {onServerReadyReader, onServerReadyWriter};

    for (auto fd: fds) {
        if (close(fd) != 0) {
            std::cerr << "close failed: errno=" << errno << std::endl;
            std::terminate();
        }
    }
}

awaitable<void> ObjectStorageServer::listener(tcp::endpoint endpoint) {
    auto executor = co_await boost::asio::this_coro::executor;
    tcp::acceptor acceptor(executor, endpoint);

    setServerReadyFd();

    for (;;) {
        auto clientSocket = std::make_shared<tcp::socket>(executor);
        co_await acceptor.async_accept(*clientSocket, use_awaitable);
        setTCPNoDelay(*clientSocket, true);

        co_spawn(executor, processRequests(clientSocket), detached);
    }
}

awaitable<void> ObjectStorageServer::processRequests(std::shared_ptr<tcp::socket> socket) {
    try {
        for (;;) {
            ObjectRequestHeader requestHeader;
            co_await readRequestHeader(socket, requestHeader);

            switch (requestHeader.requestType) {
                case ObjectRequestType::SET_OBJECT: {
                    co_await processSetRequest(socket, requestHeader);
                    break;
                }
                case ObjectRequestType::GET_OBJECT: {
                    co_await processGetRequest(socket, requestHeader);
                    break;
                }
                case ObjectRequestType::DELETE_OBJECT: {
                    co_await processDeleteRequest(socket, requestHeader);
                    break;
                }
            }
        }
    } catch (std::exception& e) {
        // TODO: Logging support
        // std::printf("process_request Exception: %s\n", e.what());
    }
}

awaitable<void> ObjectStorageServer::processSetRequest(
    std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& requestHeader) {
    if (requestHeader.payloadLength > MEMORY_LIMIT_IN_BYTES) {
        std::cerr << "payload length is larger than MEMORY_LIMIT_IN_BYTES = " << MEMORY_LIMIT_IN_BYTES << '\n';
        std::terminate();
    }

    if (requestHeader.payloadLength > SIZE_MAX) {
        std::cerr << "payload length is larger than SIZE_MAX = " << SIZE_MAX << '\n';
        std::terminate();
    }

    ObjectPayload requestPayload;
    requestPayload.resize(requestHeader.payloadLength);

    try {
        std::size_t n = co_await boost::asio::async_read(*socket, boost::asio::buffer(requestPayload), use_awaitable);
        // TODO: check the value of `n`.
    } catch (boost::system::system_error& e) {
        std::cerr << "payload ends prematurely, e.what() = " << e.what() << '\n';
        std::cerr << "Failing fast. Terminting now...\n";
        std::terminate();
    }

    auto objectPtr = objectRegister.setObject(requestHeader.objectID, std::move(requestPayload));

    co_await optionallySendPendingRequests(requestHeader.objectID, objectPtr);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::SET_O_K,
    };

    co_await writeResponse(socket, responseHeader, {});
}

awaitable<void> ObjectStorageServer::processGetRequest(
    std::shared_ptr<tcp::socket> socket, const ObjectRequestHeader& requestHeader) {
    auto objectPtr = objectRegister.getObject(requestHeader.objectID);

    if (objectPtr != nullptr) {
        co_await sendGetResponse(socket, requestHeader, objectPtr);
    } else {
        // We don't have the object yet. Send the response later after once we receive the SET request.
        pendingGetRequests[requestHeader.objectID].emplace_back(socket, requestHeader);
    }
}

awaitable<void> ObjectStorageServer::processDeleteRequest(
    std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& requestHeader) {
    bool success = objectRegister.deleteObject(requestHeader.objectID);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = success ? ObjectResponseType::DEL_O_K : ObjectResponseType::DEL_NOT_EXISTS,
    };

    co_await writeResponse(socket, responseHeader, {});
}

awaitable<void> ObjectStorageServer::readRequestHeader(
    std::shared_ptr<tcp::socket> socket, ObjectRequestHeader& header) {
    try {
        std::array<uint64_t, CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE> buffer;
        std::size_t n = co_await boost::asio::async_read(
            *socket, boost::asio::buffer(buffer.data(), CAPNP_HEADER_SIZE), use_awaitable);

        // TODO: check the value of `n`
        header = ObjectRequestHeader::fromBuffer(buffer);
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
        std::cerr << "exception thrown, header not a capnp e.what() = " << e.what() << '\n';
        throw e;
    }
}

boost::asio::awaitable<void> ObjectStorageServer::writeResponse(
    std::shared_ptr<tcp::socket> socket, ObjectResponseHeader& header, std::span<const unsigned char> payload) {
    auto headerBuffer = header.toBuffer();

    std::array<boost::asio::const_buffer, 2> buffers {
        boost::asio::buffer(headerBuffer.asBytes().begin(), headerBuffer.asBytes().size()),
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

awaitable<void> ObjectStorageServer::sendGetResponse(
    std::shared_ptr<tcp::socket> socket,
    const ObjectRequestHeader& requestHeader,
    std::shared_ptr<const ObjectPayload> objectPtr) {
    uint64_t payloadLength = std::min(static_cast<uint64_t>(objectPtr->size()), requestHeader.payloadLength);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = payloadLength,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::GET_O_K,
    };

    co_await writeResponse(socket, responseHeader, {objectPtr->data(), payloadLength});
}

awaitable<void> ObjectStorageServer::optionallySendPendingRequests(
    const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr) {
    auto it = pendingGetRequests.find(objectID);

    if (it == pendingGetRequests.end()) {
        co_return;
    }

    // Immediately remove the pending object's requests, or else another coroutine might process them too.
    auto requests = std::move(it->second);
    pendingGetRequests.erase(it);

    for (auto& request: requests) {
        if (!request.socket->is_open()) {
            continue;
        }

        co_await sendGetResponse(request.socket, request.requestHeader, objectPtr);
    }
}

};  // namespace object_storage
};  // namespace scaler
