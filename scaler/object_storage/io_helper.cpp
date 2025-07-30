#include "io_helper.h"

#include <iostream>

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
