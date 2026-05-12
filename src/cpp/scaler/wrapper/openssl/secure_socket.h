#pragma once

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace wrapper {
namespace openssl {

static const SSL_METHOD* defaultSSLMethod = TLS_method();

// A libuv-like socket implementing SSL/TLS using OpenSSL.
class SecureSocket {
public:
    enum class State {
        Uninitialized,
        Connecting,
        Handshaking,
        Established,
        Closing,
        Closed,
    };

    static std::expected<SecureSocket, uv::Error> init(
        uv::TCPSocket socket, const SSL_METHOD* method = defaultSSLMethod) noexcept;

    std::expected<uv::ConnectRequest, uv::Error> connect(
        const uv::SocketAddress& address, uv::ConnectCallback callback) noexcept;

    std::expected<void, uv::Error> readStart(uv::ReadCallback callback) noexcept;

    void readStop() noexcept;

    std::expected<void, uv::Error> write(
        std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept;

    std::expected<void, uv::Error> write(std::span<const uint8_t> buffer, uv::WriteCallback callback) noexcept;

    std::expected<void, uv::Error> shutdown(uv::ShutdownCallback callback) noexcept;

    std::expected<void, uv::Error> closeReset() noexcept;

    std::expected<uv::SocketAddress, uv::Error> getSockName() const noexcept;

    std::expected<uv::SocketAddress, uv::Error> getPeerName() const noexcept;

    std::expected<void, uv::Error> nodelay(bool enable) noexcept;

    State state() const noexcept;

    bool established() const noexcept;

    uv::TCPSocket& transport() noexcept;

private:
    // TODO: use camelCase for constants
    static constexpr size_t DEFAULT_DECRYPT_CHUNK_SIZE = 16 * 1024;

    struct PendingWrite {
        std::span<const uint8_t> _payload;
        uv::WriteCallback _callback;
    };

    SecureSocket(
        uv::TCPSocket socket,
        SSLPtr<SSL_CTX> context,
        SSLPtr<SSL> ssl,
        SSLPtr<BIO> readBIO,
        SSLPtr<BIO> writeBIO) noexcept;

    std::expected<void, uv::Error> tryFinishHandshake() noexcept;

    std::expected<void, uv::Error> tryFinishShutdown() noexcept;

    std::expected<void, uv::Error> flushToApplication() noexcept;

    std::expected<void, uv::Error> flushToTransport() noexcept;

    std::expected<void, uv::Error> processPendingWrites() noexcept;

    void failPendingWrites(uv::Error error) noexcept;

    void failWithError(uv::Error error) noexcept;

    void onSSLError(int sslError) noexcept;

    void onTransportError(uv::Error error) noexcept;

    void onTransportConnected(std::expected<void, uv::Error> result, uv::ConnectCallback callback) noexcept;

    void onTransportRead(std::expected<std::span<const uint8_t>, uv::Error> result) noexcept;

    uv::TCPSocket _transport;
    State _state {State::Uninitialized};

    SSLPtr<SSL_CTX> _context {};
    SSLPtr<SSL> _ssl {};
    SSLPtr<BIO> _readBIO {};
    SSLPtr<BIO> _writeBIO {};

    std::optional<uv::ReadCallback> _onReadCallback {};
    std::optional<uv::ShutdownCallback> _shutdownCallback {};

    std::deque<PendingWrite> _pendingWrites {};
};

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler
