#pragma once

#include <expected>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/internal/websocket_stream.h"

namespace scaler {
namespace ymq {
namespace internal {

// A libuv-like server socket implementing the WebSocket protocol.
class WebSocketServer {
public:
    static std::expected<WebSocketServer, scaler::wrapper::uv::Error> init(scaler::wrapper::uv::Loop& loop) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> bind(
        const scaler::wrapper::uv::SocketAddress& address, uv_tcp_flags flags) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> listen(
        int backlog, scaler::wrapper::uv::ConnectionCallback callback) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> accept(WebSocketStream& connection) noexcept;

    std::expected<scaler::wrapper::uv::SocketAddress, scaler::wrapper::uv::Error> getSockName() const noexcept;

private:
    WebSocketServer(scaler::wrapper::uv::TCPServer server) noexcept;

    scaler::wrapper::uv::TCPServer _server;
};

}  // namespace internal
}  // namespace ymq
}  // namespace scaler
