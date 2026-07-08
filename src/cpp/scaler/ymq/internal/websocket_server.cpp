#include "scaler/ymq/internal/websocket_server.h"

#include <utility>
#include <variant>

namespace scaler {
namespace ymq {
namespace internal {

std::expected<WebSocketServer, scaler::wrapper::uv::Error> WebSocketServer::init(
    scaler::wrapper::uv::Loop& loop, std::optional<scaler::wrapper::openssl::SSLContext> sslContext) noexcept
{
    Server server;

    if (sslContext.has_value()) {
        auto secureServer = scaler::wrapper::openssl::SecureServer::init(loop);
        if (!secureServer.has_value()) {
            return std::unexpected {secureServer.error()};
        }
        server = std::move(secureServer.value());
    } else {
        auto tcpServer = scaler::wrapper::uv::TCPServer::init(loop);
        if (!tcpServer.has_value()) {
            return std::unexpected {tcpServer.error()};
        }
        server = std::move(tcpServer.value());
    }

    return WebSocketServer {std::move(server), std::move(sslContext)};
}

WebSocketServer::WebSocketServer(Server server, std::optional<scaler::wrapper::openssl::SSLContext> sslContext) noexcept
    : _server(std::move(server)), _sslContext(std::move(sslContext))
{
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::bind(
    const WebSocketAddress& address, uv_tcp_flags flags) noexcept
{
    _address = address;
    return std::visit([&](auto& server) { return server.bind(address.tcpAddress, flags); }, _server);
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::listen(
    int backlog, scaler::wrapper::uv::ConnectionCallback callback) noexcept
{
    return std::visit([&](auto& server) { return server.listen(backlog, std::move(callback)); }, _server);
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::accept(
    WebSocketStream& connection, WebSocketStream::HandshakeDoneCallback callback) noexcept
{
    auto acceptResult = std::visit([&](auto& server) { return server.accept(connection.transport()); }, _server);
    if (!acceptResult.has_value()) {
        return acceptResult;
    }

    return connection.accept(std::move(callback));
}

std::expected<WebSocketAddress, scaler::wrapper::uv::Error> WebSocketServer::getSockName() const noexcept
{
    if (!_address.has_value()) {
        // getSockName() called before bind()
        return std::unexpected {scaler::wrapper::uv::Error {UV_EINVAL}};
    }

    auto tcpAddress = std::visit([](const auto& server) { return server.getSockName(); }, _server);
    if (!tcpAddress.has_value()) {
        return std::unexpected {tcpAddress.error()};
    }

    // Reconstruct the WebSocket address with the actual bound port (handles port 0 auto-assignment).
    WebSocketAddress address = _address.value();
    address.tcpAddress       = tcpAddress.value();
    address.port             = static_cast<uint16_t>(tcpAddress->port());
    return address;
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler
