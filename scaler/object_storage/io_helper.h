#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <span>

#include "scaler/object_storage/defs.h"

namespace scaler {
namespace object_storage {

void setTCPNoDelay(boost::asio::ip::tcp::socket& socket, bool isNoDelay);

boost::asio::awaitable<void> readRequestHeader(boost::asio::ip::tcp::socket& socket, ObjectRequestHeader& header);

boost::asio::awaitable<void> readRequestPayload(
    boost::asio::ip::tcp::socket& socket, ObjectRequestHeader& header, ObjectPayload& payload);

boost::asio::awaitable<void> writeResponse(
    boost::asio::ip::tcp::socket& socket, ObjectResponseHeader& header, std::span<const unsigned char> payloadView);

};  // namespace object_storage
};  // namespace scaler
