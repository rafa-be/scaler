#include "io_helper.h"

#include <capnp/message.h>
#include <capnp/serialize.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/system_error.hpp>
#include <cstdint>
#include <exception>
#include <iostream>

#include "protocol/object_storage.capnp.h"
#include "scaler/object_storage/constants.h"
#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/message.h"

using boost::asio::awaitable;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

namespace scaler {
namespace object_storage {

void setTCPNoDelay(tcp::socket& socket, bool isNoDelay) {
    boost::system::error_code ec;
    socket.set_option(tcp::no_delay(isNoDelay), ec);

    if (ec) {
        std::cerr << "failed to set TCP_NODELAY on client socket: " << ec.message() << std::endl;
        std::terminate();
    }
}

};  // namespace object_storage
};  // namespace scaler
