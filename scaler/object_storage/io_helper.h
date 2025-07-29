#pragma once

#include <arpa/inet.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <span>

#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/message.h"

namespace scaler {
namespace object_storage {

void setTCPNoDelay(boost::asio::ip::tcp::socket& socket, bool isNoDelay);

};  // namespace object_storage
};  // namespace scaler
