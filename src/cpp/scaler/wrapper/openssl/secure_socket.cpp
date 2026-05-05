#include "scaler/wrapper/openssl/secure_socket.h"

#include <array>
#include <cassert>
#include <functional>
#include <utility>

namespace scaler {
namespace wrapper {
namespace openssl {

std::expected<SecureSocket, uv::Error> SecureSocket::init(uv::Loop& loop) noexcept
{
    std::expected<uv::TCPSocket, uv::Error> socketResult = uv::TCPSocket::init(loop);
    if (!socketResult.has_value()) {
        return std::unexpected {socketResult.error()};
    }

    SecureSocket socket {std::move(socketResult.value())};

    // TODO: make createContext a static method returning a SSLContextPtr and being called before SecureSocket
    // constructor
    if (std::expected<void, uv::Error> contextResult = socket.createContext(); !contextResult.has_value()) {
        return std::unexpected(contextResult.error());
    }

    return socket;
}

SecureSocket::SecureSocket(uv::TCPSocket socket) noexcept: _socket(std::move(socket))
{
}

std::expected<void, uv::Error> SecureSocket::readStart(uv::ReadCallback callback) noexcept
{
    _onRead      = std::move(callback);
    _readEnabled = true;

    return startTransportRead();
}

std::expected<uv::ConnectRequest, uv::Error> SecureSocket::connect(
    const uv::SocketAddress& address, uv::ConnectCallback callback) noexcept
{
    _state = State::Connecting;

    auto onConnect = [this, callback = std::move(callback)](std::expected<void, uv::Error> result) mutable {
        if (!result.has_value()) {
            _state = State::Closed;
            callback(std::unexpected {result.error()});
            return;
        }

        std::expected<void, uv::Error> tlsObjectResult = createTLSObjects();
        if (!tlsObjectResult.has_value()) {
            _state = State::Closed;
            callback(std::move(tlsObjectResult));
            return;
        }

        std::expected<void, uv::Error> readResult = startTransportRead();
        if (!readResult.has_value()) {
            _state = State::Closed;
            callback(std::move(readResult));
            return;
        }

        std::expected<void, uv::Error> handshakeResult = startHandshake();
        if (!handshakeResult.has_value()) {
            _state = State::Closed;
            callback(std::move(handshakeResult));
            return;
        }

        callback({});
    };

    std::expected<uv::ConnectRequest, uv::Error> connectResult = _socket.connect(address, std::move(onConnect));
    if (!connectResult.has_value()) {
        _state = State::Uninitialized;
        return connectResult;
    }

    return connectResult;
}

void SecureSocket::readStop() noexcept
{
    _socket.readStop();
    _readEnabled = false;
}

std::expected<void, uv::Error> SecureSocket::closeReset() noexcept
{
    return _socket.closeReset();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getSockName() const noexcept
{
    return _socket.getSockName();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getPeerName() const noexcept
{
    return _socket.getPeerName();
}

std::expected<void, uv::Error> SecureSocket::nodelay(bool enable) noexcept
{
    return _socket.nodelay(enable);
}

std::expected<void, uv::Error> SecureSocket::write(
    std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept
{
    // TODO: move queueWrite content in this function if it's not used elsewhere
    std::expected<void, uv::Error> queueResult = queueWrite(buffers, std::move(callback));
    if (!queueResult.has_value()) {
        return queueResult;
    }

    return processPendingPlaintextWrites();
}

std::expected<void, uv::Error> SecureSocket::write(std::span<const uint8_t> buffer, uv::WriteCallback callback) noexcept
{
    const std::span<const std::span<const uint8_t>> buffers {&buffer, 1};
    return write(buffers, std::move(callback));
}

std::expected<void, uv::Error> SecureSocket::shutdown(uv::ShutdownCallback callback) noexcept
{
    _state = State::Closing;

    std::expected<void, uv::Error> closeNotifyResult = sendCloseNotify();
    if (!closeNotifyResult.has_value()) {
        callback(std::unexpected {closeNotifyResult.error()});
        return closeNotifyResult;
    }

    std::expected<void, uv::Error> flushResult = flushCiphertextBIO();
    if (!flushResult.has_value()) {
        callback(std::unexpected {flushResult.error()});
        return flushResult;
    }

    std::expected<void, uv::Error> processResult = processPendingCiphertextWrites();
    if (!processResult.has_value()) {
        callback(std::unexpected {processResult.error()});
        return processResult;
    }

    std::expected<uv::ShutdownRequest, uv::Error> shutdownResult = _socket.shutdown(std::move(callback));
    if (!shutdownResult.has_value()) {
        return std::unexpected {shutdownResult.error()};
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::createContext() noexcept
{
    const SSL_METHOD* method = TLS_client_method();
    if (method == nullptr) {
        return std::unexpected {uv::Error {UV_EPROTO}};
    }

    SSL_CTX* context = SSL_CTX_new(method);
    if (context == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    _context = SSLContextPtr {context, &SSL_CTX_free};

    return {};
}

// TODO: rename it initOpenSSLObjects
std::expected<void, uv::Error> SecureSocket::createTLSObjects() noexcept
{
    if (_context == nullptr) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    SSL* ssl = SSL_new(_context.get());
    if (ssl == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    BIO* readBIO  = BIO_new(BIO_s_mem());
    BIO* writeBIO = BIO_new(BIO_s_mem());
    if (readBIO == nullptr || writeBIO == nullptr) {
        // TODO: rely on unique_ptr internals instead of freeing these manually, only std::moving them to the member
        // pointers once all succeeded.
        if (readBIO != nullptr) {
            BIO_free(readBIO);
        }
        if (writeBIO != nullptr) {
            BIO_free(writeBIO);
        }
        SSL_free(ssl);
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    _ssl      = SSLPtr {ssl, &SSL_free};
    _readBIO  = BIOPtr {readBIO, &BIO_free};
    _writeBIO = BIOPtr {writeBIO, &BIO_free};

    BIO_up_ref(_readBIO.get());
    BIO_up_ref(_writeBIO.get());
    SSL_set_bio(_ssl.get(), _readBIO.get(), _writeBIO.get());

    SSL_set_connect_state(_ssl.get());

    return {};
}

std::expected<void, uv::Error> SecureSocket::startTransportRead() noexcept
{
    if (_transportReadStarted) {
        return {};
    }

    std::expected<void, uv::Error> readResult =
        _socket.readStart(std::bind_front(&SecureSocket::onTransportRead, this));
    if (!readResult.has_value()) {
        return std::unexpected {readResult.error()};
    }

    _transportReadStarted = true;
    return {};
}

std::expected<void, uv::Error> SecureSocket::startHandshake() noexcept
{
    _state = State::Handshaking;
    return driveHandshake();
}

// TODO: rename tryHandshake()
std::expected<void, uv::Error> SecureSocket::driveHandshake() noexcept
{
    assert(_state == State::Handshaking);

    if (_ssl == nullptr) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    const int status = SSL_do_handshake(_ssl.get());
    if (status != 1) {
        const int sslError = SSL_get_error(_ssl.get(), status);
        if (sslError == SSL_ERROR_WANT_READ || sslError == SSL_ERROR_WANT_WRITE) {
            std::expected<void, uv::Error> flushResult = flushCiphertextBIO();
            if (!flushResult.has_value()) {
                return flushResult;
            }

            return processPendingCiphertextWrites();
        }

        return std::unexpected {uv::Error {UV_EPROTO}};
    }

    _state = State::Established;

    std::expected<void, uv::Error> flushResult = flushCiphertextBIO();
    if (!flushResult.has_value()) {
        return flushResult;
    }

    return processPendingCiphertextWrites();
}

std::expected<void, uv::Error> SecureSocket::drainPlaintextReads() noexcept
{
    if (_ssl == nullptr || !_readEnabled || !_onRead) {
        return {};
    }

    std::array<uint8_t, DEFAULT_DECRYPT_CHUNK_SIZE> buffer {};

    while (true) {
        const int readCount = SSL_read(_ssl.get(), buffer.data(), static_cast<int>(buffer.size()));
        if (readCount <= 0) {
            const int sslError = SSL_get_error(_ssl.get(), readCount);
            if (sslError == SSL_ERROR_WANT_READ || sslError == SSL_ERROR_WANT_WRITE) {
                return {};
            }

            if (sslError == SSL_ERROR_ZERO_RETURN) {
                _onRead(std::unexpected {uv::Error {UV_EOF}});
                return {};
            }

            return std::unexpected {uv::Error {UV_EPROTO}};
        }

        _onRead(std::span<const uint8_t> {buffer.data(), static_cast<size_t>(readCount)});
    }
}

std::expected<void, uv::Error> SecureSocket::queueWrite(
    std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept
{
    PendingPlaintextWrite pendingWrite {};

    // Copies buffers

    size_t totalSize = 0;
    for (const std::span<const uint8_t>& buffer: buffers) {
        totalSize += buffer.size();
    }

    pendingWrite._payload.reserve(totalSize);
    for (const std::span<const uint8_t>& buffer: buffers) {
        pendingWrite._payload.insert(pendingWrite._payload.end(), buffer.begin(), buffer.end());
    }

    pendingWrite._callback = std::move(callback);

    _pendingPlaintextBytes += pendingWrite._payload.size();
    _pendingPlaintextWrites.push_back(std::move(pendingWrite));

    // TODO: remove this max pending bytes limit. Let the app OoM
    if (_pendingPlaintextBytes > _maxPendingBytes) {
        _pendingPlaintextBytes -= _pendingPlaintextWrites.back()._payload.size();
        _pendingPlaintextWrites.pop_back();
        return std::unexpected {uv::Error {UV_ENOBUFS}};
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::processPendingPlaintextWrites() noexcept
{
    if (_state != State::Established) {
        return {};
    }

    while (!_pendingPlaintextWrites.empty()) {
        PendingPlaintextWrite& pendingWrite = _pendingPlaintextWrites.front();

        if (pendingWrite._payload.empty()) {
            pendingWrite._callback({});
            _pendingPlaintextWrites.pop_front();
            continue;
        }

        // TODO: make the algorithm work with buffers larger than INTMAX
        const int status =
            SSL_write(_ssl.get(), pendingWrite._payload.data(), static_cast<int>(pendingWrite._payload.size()));

        if (status <= 0) {
            const int sslError = SSL_get_error(_ssl.get(), status);
            if (sslError == SSL_ERROR_WANT_READ || sslError == SSL_ERROR_WANT_WRITE) {
                break;
            } else {
                return std::unexpected {uv::Error {UV_EPROTO}};
            }
        }

        _pendingPlaintextBytes -= static_cast<size_t>(status);

        if (static_cast<size_t>(status) >= pendingWrite._payload.size()) {
            pendingWrite._callback({});
            _pendingPlaintextWrites.pop_front();
        } else {
            // TODO: remove the O(n) of erase() by either using a std::span in the pendingWrite (and a backing up
            // vector) or a pendingWrite._offset value.
            pendingWrite._payload.erase(pendingWrite._payload.begin(), pendingWrite._payload.begin() + status);
        }
    }

    std::expected<void, uv::Error> flushResult = flushCiphertextBIO();
    if (!flushResult.has_value()) {
        return flushResult;
    }

    return processPendingCiphertextWrites();
}

std::expected<void, uv::Error> SecureSocket::flushCiphertextBIO() noexcept
{
    if (_writeBIO == nullptr) {
        return {};
    }

    std::array<char, DEFAULT_DECRYPT_CHUNK_SIZE> buffer {};

    while (true) {
        const int readCount = BIO_read(_writeBIO.get(), buffer.data(), static_cast<int>(buffer.size()));
        if (readCount <= 0) {
            break;
        }

        // TODO: I think it's safe to call socket._write() directly here

        PendingCiphertextWrite pendingWrite {};
        pendingWrite._id = _nextWriteId++;
        pendingWrite._payload.assign(buffer.begin(), buffer.begin() + readCount);
        pendingWrite._callback = []([[maybe_unused]] std::expected<void, uv::Error> result) noexcept {};

        _pendingCiphertextBytes += pendingWrite._payload.size();
        _pendingCiphertextWrites.push_back(std::move(pendingWrite));
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::processPendingCiphertextWrites() noexcept
{
    if (_underlyingWriteInFlight) {
        return {};
    }

    if (_pendingCiphertextWrites.empty()) {
        return {};
    }

    PendingCiphertextWrite& pendingWrite = _pendingCiphertextWrites.front();

    const std::span<const uint8_t> buffer {pendingWrite._payload.data(), pendingWrite._payload.size()};
    std::expected<uv::WriteRequest, uv::Error> writeResult =
        _socket.write(buffer, [this, writeId = pendingWrite._id](std::expected<void, uv::Error> result) mutable {
            onUnderlyingWriteDone(writeId, std::move(result));
        });

    if (!writeResult.has_value()) {
        return std::unexpected {writeResult.error()};
    }

    _underlyingWriteInFlight = true;

    return {};
}

void SecureSocket::onTransportRead(std::expected<std::span<const uint8_t>, uv::Error> result) noexcept
{
    if (!result.has_value()) {
        failPendingWrites(result.error());
        return;
    }

    if (_ssl != nullptr && _readBIO != nullptr) {
        const std::span<const uint8_t> data = result.value();
        BIO_write(_readBIO.get(), reinterpret_cast<const char*>(data.data()), static_cast<int>(data.size()));
    }

    if (_state == State::Handshaking) {
        std::expected<void, uv::Error> handshakeResult = driveHandshake();
        if (!handshakeResult.has_value()) {
            failPendingWrites(handshakeResult.error());
            return;
        }
    }

    std::expected<void, uv::Error> readResult = drainPlaintextReads();
    if (!readResult.has_value()) {
        failPendingWrites(readResult.error());
        return;
    }

    if (_state == State::Established) {
        std::expected<void, uv::Error> writeResult = processPendingPlaintextWrites();
        if (!writeResult.has_value()) {
            failPendingWrites(writeResult.error());
            return;
        }
    }
}

void SecureSocket::onUnderlyingWriteDone(size_t writeId, std::expected<void, uv::Error> result) noexcept
{
    _underlyingWriteInFlight = false;

    if (_pendingCiphertextWrites.empty()) {
        return;
    }

    PendingCiphertextWrite pendingWrite = std::move(_pendingCiphertextWrites.front());
    _pendingCiphertextWrites.pop_front();

    _pendingCiphertextBytes -= pendingWrite._payload.size();

    if (pendingWrite._id != writeId) {
        failPendingWrites(uv::Error {UV_EPROTO});
        return;
    }

    pendingWrite._callback(std::move(result));

    if (!result.has_value()) {
        failPendingWrites(result.error());
        return;
    }

    std::expected<void, uv::Error> processResult = processPendingCiphertextWrites();
    if (!processResult.has_value()) {
        failPendingWrites(processResult.error());
    }
}

// TODO: Rename onWriteFailure
void SecureSocket::failPendingWrites(uv::Error error) noexcept
{
    while (!_pendingPlaintextWrites.empty()) {
        PendingPlaintextWrite pendingWrite = std::move(_pendingPlaintextWrites.front());
        _pendingPlaintextWrites.pop_front();

        _pendingPlaintextBytes -= pendingWrite._payload.size();
        pendingWrite._callback(std::unexpected {error});
    }

    while (!_pendingCiphertextWrites.empty()) {
        PendingCiphertextWrite pendingWrite = std::move(_pendingCiphertextWrites.front());
        _pendingCiphertextWrites.pop_front();

        _pendingCiphertextBytes -= pendingWrite._payload.size();
        pendingWrite._callback(std::unexpected {error});
    }
}

std::expected<void, uv::Error> SecureSocket::sendCloseNotify() noexcept
{
    if (_ssl == nullptr) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    if (_closeNotifySent) {
        return {};
    }

    const int status = SSL_shutdown(_ssl.get());
    if (status < 0) {
        const int sslError = SSL_get_error(_ssl.get(), status);
        if (sslError != SSL_ERROR_WANT_READ && sslError != SSL_ERROR_WANT_WRITE) {
            return std::unexpected {uv::Error {UV_EPROTO}};
        }
    }

    _closeNotifySent = true;
    return {};
}

SecureSocket::State SecureSocket::state() const noexcept
{
    return _state;
}

bool SecureSocket::established() const noexcept
{
    return _state == State::Established;
}

uv::TCPSocket& SecureSocket::tcpSocket() noexcept
{
    return _socket;
}

const uv::TCPSocket& SecureSocket::tcpSocket() const noexcept
{
    return _socket;
}

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler
