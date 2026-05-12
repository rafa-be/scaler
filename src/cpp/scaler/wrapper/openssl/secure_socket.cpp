#include "scaler/wrapper/openssl/secure_socket.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <climits>
#include <functional>
#include <utility>
#include <vector>

namespace scaler {
namespace wrapper {
namespace openssl {

std::expected<SecureSocket, uv::Error> SecureSocket::init(uv::TCPSocket socket, const SSL_METHOD* method) noexcept
{
    if (method == nullptr) {
        return std::unexpected {uv::Error {UV_EPROTO}};
    }

    // Create SSL context
    SSLPtr<SSL_CTX> context {SSL_CTX_new(method)};
    if (context == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    // Create SSL session
    SSLPtr<SSL> ssl {SSL_new(context.get())};
    if (ssl == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    // Create and configure BIO buffers
    SSLPtr<BIO> readBIO {BIO_new(BIO_s_mem())};
    SSLPtr<BIO> writeBIO {BIO_new(BIO_s_mem())};
    if (readBIO == nullptr || writeBIO == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    // Configure SSL with BIOs.
    BIO_up_ref(readBIO.get());  // BIO_up_ref() is required as SSL_set_bio() takes ownership of these buffers.
    BIO_up_ref(writeBIO.get());
    SSL_set_bio(ssl.get(), readBIO.get(), writeBIO.get());

    return SecureSocket {
        std::move(socket), std::move(context), std::move(ssl), std::move(readBIO), std::move(writeBIO)};
}

SecureSocket::SecureSocket(
    uv::TCPSocket socket, SSLPtr<SSL_CTX> context, SSLPtr<SSL> ssl, SSLPtr<BIO> readBIO, SSLPtr<BIO> writeBIO) noexcept
    : _transport(std::move(socket))
    , _context(std::move(context))
    , _ssl(std::move(ssl))
    , _readBIO(std::move(readBIO))
    , _writeBIO(std::move(writeBIO))
{
}

std::expected<uv::ConnectRequest, uv::Error> SecureSocket::connect(
    const uv::SocketAddress& address, uv::ConnectCallback callback) noexcept
{
    assert(_state == State::Uninitialized);

    _state = State::Connecting;

    std::expected<uv::ConnectRequest, uv::Error> result =
        _transport.connect(address, std::bind_front(&SecureSocket::onTransportConnected, this, std::move(callback)));
    if (!result.has_value()) {
        onTransportError(result.error());
        return result;
    }

    return result;
}

std::expected<void, uv::Error> SecureSocket::readStart(uv::ReadCallback callback) noexcept
{
    // We do not propagate readStart() to the transport layer, as we already enabled it during the SSL handshake.
    // We instead "mask" and queue the reads until the application layer register a callback.

    _onReadCallback = std::move(callback);

    return flushToApplication();
}

void SecureSocket::readStop() noexcept
{
    _onReadCallback.reset();
}

std::expected<void, uv::Error> SecureSocket::write(
    std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept
{
    if (buffers.empty()) {
        callback({});
        return {};
    }

    // Queue each buffer as a single PendingWrite object.
    for (const auto& buffer: buffers) {
        // Only attach the callback to the last buffer; earlier get a noop.
        auto bufferCallback = &buffer == &buffers.back() ? std::move(callback) : [](std::expected<void, uv::Error>) {};

        _pendingWrites.push_back(PendingWrite {buffer, std::move(bufferCallback)});
    }

    return processPendingWrites();
}

std::expected<void, uv::Error> SecureSocket::write(std::span<const uint8_t> buffer, uv::WriteCallback callback) noexcept
{
    const std::span<const std::span<const uint8_t>> buffers {&buffer, 1};
    return write(buffers, std::move(callback));
}

std::expected<void, uv::Error> SecureSocket::shutdown(uv::ShutdownCallback callback) noexcept
{
    _state            = State::Closing;
    _shutdownCallback = std::move(callback);

    failPendingWrites(uv::Error {UV_ECANCELED});

    return tryFinishShutdown();
}

std::expected<void, uv::Error> SecureSocket::closeReset() noexcept
{
    _state = State::Closed;

    failPendingWrites(uv::Error {UV_ECANCELED});

    return _transport.closeReset();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getSockName() const noexcept
{
    return _transport.getSockName();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getPeerName() const noexcept
{
    return _transport.getPeerName();
}

std::expected<void, uv::Error> SecureSocket::nodelay(bool enable) noexcept
{
    return _transport.nodelay(enable);
}

SecureSocket::State SecureSocket::state() const noexcept
{
    return _state;
}

bool SecureSocket::established() const noexcept
{
    return _state == State::Established;
}

uv::TCPSocket& SecureSocket::transport() noexcept
{
    return _transport;
}

std::expected<void, uv::Error> SecureSocket::tryFinishHandshake() noexcept
{
    assert(_state == State::Handshaking);

    const int status = SSL_do_handshake(_ssl.get());

    std::expected<void, uv::Error> flushResult = flushToTransport();
    if (!flushResult.has_value()) {
        return flushResult;
    }

    if (status != 1) {
        const int sslError = SSL_get_error(_ssl.get(), status);

        if (sslError == SSL_ERROR_WANT_WRITE) {
            return tryFinishHandshake();
        } else if (sslError == SSL_ERROR_WANT_READ) {
            return {};  // try again later
        } else {
            onSSLError(sslError);
            return std::unexpected {uv::Error {UV_EPROTO}};
        }
    }

    assert(status == 1);

    _state = State::Established;

    return {};
}

std::expected<void, uv::Error> SecureSocket::tryFinishShutdown() noexcept
{
    assert(_state == State::Closing);
    assert(_shutdownCallback.has_value());

    const int status = SSL_shutdown(_ssl.get());

    std::expected<void, uv::Error> flushResult = flushToTransport();
    if (!flushResult.has_value()) {
        (*_shutdownCallback)(std::unexpected {flushResult.error()});
        _shutdownCallback.reset();
        return flushResult;
    }

    if (status == 0) {
        // Shutdown sent but we haven't received the response from the other peer yet. Try again later.
        return {};
    }

    if (status < 0) {
        const int sslError = SSL_get_error(_ssl.get(), status);

        if (sslError == SSL_ERROR_WANT_WRITE) {
            return tryFinishShutdown();  // try again
        } else if (sslError == SSL_ERROR_WANT_READ) {
            return {};  // try again later
        } else {
            onSSLError(sslError);
            uv::Error error {UV_EPROTO};
            (*_shutdownCallback)(std::unexpected {error});
            _shutdownCallback.reset();
            return std::unexpected {error};
        }
    }

    assert(status == 1);

    // SSL shutdown completed, shut down the transport.
    std::expected<uv::ShutdownRequest, uv::Error> shutdownResult = _transport.shutdown(std::move(*_shutdownCallback));
    if (!shutdownResult.has_value()) {
        return std::unexpected {shutdownResult.error()};
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::flushToApplication() noexcept
{
    std::array<uint8_t, DEFAULT_DECRYPT_CHUNK_SIZE> buffer {};

    while (_onReadCallback.has_value()) {  // Stops if a callback calls readStop().
        const int readCount = SSL_read(_ssl.get(), buffer.data(), static_cast<int>(buffer.size()));
        if (readCount <= 0) {
            const int sslError = SSL_get_error(_ssl.get(), readCount);
            if (sslError == SSL_ERROR_WANT_READ || sslError == SSL_ERROR_WANT_WRITE) {
                return {};
            } else if (sslError == SSL_ERROR_ZERO_RETURN) {
                // Peer requested shutdown
                onTransportError(uv::Error {UV_EOF});
                return {};
            } else {
                onSSLError(sslError);
                return std::unexpected {uv::Error {UV_EPROTO}};
            }
        }

        (*_onReadCallback)(std::span<const uint8_t> {buffer.data(), static_cast<size_t>(readCount)});
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::flushToTransport() noexcept
{
    for (;;) {
        const size_t pending = BIO_ctrl_pending(_writeBIO.get());
        if (pending == 0) {
            return {};
        }

        auto buffer = std::make_unique<std::vector<uint8_t>>(pending);

        const int readCount =
            BIO_read(_writeBIO.get(), reinterpret_cast<char*>(buffer->data()), static_cast<int>(buffer->size()));
        if (readCount <= 0) {
            return {};
        }

        buffer->resize(static_cast<size_t>(readCount));

        const std::span<const uint8_t> bufferSpan {*buffer};

        // Callback captures the buffer to extend its lifetime until the write completes
        // TODO: handle write errors?
        auto writeResult = _transport.write(
            bufferSpan, [buffer = std::move(buffer)](std::expected<void, uv::Error> result) mutable noexcept {});
        if (!writeResult.has_value()) {
            onTransportError(writeResult.error());
            return std::unexpected {writeResult.error()};
        }
    }
}

std::expected<void, uv::Error> SecureSocket::processPendingWrites() noexcept
{
    if (_state != State::Established) {
        return {};
    }

    while (!_pendingWrites.empty()) {
        PendingWrite& pendingWrite             = _pendingWrites.front();
        const std::span<const uint8_t> payload = pendingWrite._payload;

        if (payload.empty()) {
            pendingWrite._callback({});
            _pendingWrites.pop_front();
            continue;
        }

        // SSL only supports writes up to INT_MAX.
        const int writeSize = static_cast<int>(std::min(payload.size(), static_cast<size_t>(INT_MAX)));

        const int status = SSL_write(_ssl.get(), payload.data(), writeSize);

        if (status <= 0) {
            const int sslError = SSL_get_error(_ssl.get(), status);
            if (sslError == SSL_ERROR_WANT_READ) {
                break;
            } else if (sslError == SSL_ERROR_WANT_WRITE) {
                // SSL needs to write to transport. Flush then retry.
                std::expected<void, uv::Error> flushResult = flushToTransport();
                if (!flushResult.has_value()) {
                    return flushResult;
                }
                continue;
            } else {
                onSSLError(sslError);
                return std::unexpected {uv::Error {UV_EPROTO}};
            }
        }

        const size_t bytesWritten = static_cast<size_t>(status);

        if (bytesWritten >= payload.size()) {
            pendingWrite._callback({});
            _pendingWrites.pop_front();
        } else {
            pendingWrite._payload = payload.subspan(bytesWritten);
        }
    }

    return flushToTransport();
}

void SecureSocket::failPendingWrites(uv::Error error) noexcept
{
    for (PendingWrite& pendingWrite: _pendingWrites) {
        pendingWrite._callback(std::unexpected {error});
    }
    _pendingWrites.clear();
}

void SecureSocket::failWithError(uv::Error error) noexcept
{
    _state = State::Closed;

    // Fail pending reads
    if (_onReadCallback.has_value()) {
        (*_onReadCallback)(std::unexpected {error});
    }
    readStop();

    failPendingWrites(error);

    _shutdownCallback.reset();
}

void SecureSocket::onSSLError([[maybe_unused]] int sslError) noexcept
{
    failWithError({uv::Error {UV_EPROTO}});
}

void SecureSocket::onTransportError(uv::Error error) noexcept
{
    failWithError(error);
}

void SecureSocket::onTransportConnected(std::expected<void, uv::Error> result, uv::ConnectCallback callback) noexcept
{
    if (!result.has_value()) {
        onTransportError(result.error());
        callback(std::unexpected {result.error()});
        return;
    }

    // Set SSL to connect state before starting handshake
    SSL_set_connect_state(_ssl.get());

    std::expected<void, uv::Error> readResult =
        _transport.readStart(std::bind_front(&SecureSocket::onTransportRead, this));
    if (!readResult.has_value()) {
        onTransportError(readResult.error());
        callback(std::move(readResult));
        return;
    }

    _state = State::Handshaking;

    std::expected<void, uv::Error> handshakeResult = tryFinishHandshake();
    if (!handshakeResult.has_value()) {
        callback(std::move(handshakeResult));
        return;
    }

    callback({});
}

void SecureSocket::onTransportRead(std::expected<std::span<const uint8_t>, uv::Error> result) noexcept
{
    if (!result.has_value()) {
        onTransportError(result.error());
        return;
    }

    const std::span<const uint8_t> data = result.value();
    BIO_write(_readBIO.get(), reinterpret_cast<const char*>(data.data()), static_cast<int>(data.size()));

    std::expected<void, uv::Error> drainResult = flushToApplication();
    if (!drainResult.has_value()) {
        return;
    }

    switch (_state) {
        case State::Handshaking: {
            std::expected<void, uv::Error> handshakeResult = tryFinishHandshake();
            if (!handshakeResult.has_value()) {
                return;
            }
            break;
        }
        case State::Closing: {
            std::expected<void, uv::Error> shutdownResult = tryFinishShutdown();
            if (!shutdownResult.has_value()) {
                return;
            }
            break;
        }
        case State::Established: {
            std::expected<void, uv::Error> writeResult = processPendingWrites();
            if (!writeResult.has_value()) {
                return;
            }
            break;
        }
        default: std::unreachable();
    }
}

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler
