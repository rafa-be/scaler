#include "scaler/ymq/internal/websocket_server.h"

#include <utility>

namespace scaler {
namespace ymq {
namespace internal {

std::expected<WebSocketServer, scaler::wrapper::uv::Error> WebSocketServer::init(
    scaler::wrapper::uv::Loop& loop) noexcept
{
    std::expected<scaler::wrapper::uv::TCPServer, scaler::wrapper::uv::Error> server =
        scaler::wrapper::uv::TCPServer::init(loop);
    if (!server.has_value()) {
        return std::unexpected {server.error()};
    }

    return WebSocketServer {std::move(server.value())};
}

WebSocketServer::WebSocketServer(scaler::wrapper::uv::TCPServer server) noexcept: _server(std::move(server))
{
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::bind(
    const scaler::wrapper::uv::SocketAddress& address, uv_tcp_flags flags) noexcept
{
    return _server.bind(address, flags);
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::listen(
    int backlog, scaler::wrapper::uv::ConnectionCallback callback) noexcept
{
    return _server.listen(backlog, std::move(callback));
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::accept(WebSocketStream& connection) noexcept
{
    std::expected<void, scaler::wrapper::uv::Error> acceptResult = _server.accept(connection.transport());
    if (!acceptResult.has_value()) {
        return acceptResult;
    }

    return connection.accept([](std::expected<void, scaler::wrapper::uv::Error>) {});
}

std::expected<scaler::wrapper::uv::SocketAddress, scaler::wrapper::uv::Error> WebSocketServer::getSockName()
    const noexcept
{
    return _server.getSockName();
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler
