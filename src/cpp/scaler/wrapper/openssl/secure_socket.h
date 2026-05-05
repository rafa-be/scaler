#pragma once

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <span>
#include <vector>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace wrapper {
namespace openssl {

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

    static std::expected<SecureSocket, uv::Error> init(uv::Loop& loop) noexcept;

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

    uv::TCPSocket& tcpSocket() noexcept;

    const uv::TCPSocket& tcpSocket() const noexcept;

private:
    struct PendingPlaintextWrite {
        std::vector<uint8_t> _payload;
        uv::WriteCallback _callback;
    };

    struct PendingCiphertextWrite {
        std::vector<uint8_t> _payload;
        uv::WriteCallback _callback;
        size_t _id {0};
    };

    using SSLContextPtr = std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)>;
    using SSLPtr        = std::unique_ptr<SSL, decltype(&SSL_free)>;
    using BIOPtr        = std::unique_ptr<BIO, decltype(&BIO_free)>;

    // TODO: use camelCase for constants
    static constexpr size_t DEFAULT_MAX_PENDING_BYTES  = 16 * 1024 * 1024;
    static constexpr size_t DEFAULT_DECRYPT_CHUNK_SIZE = 16 * 1024;

    explicit SecureSocket(uv::TCPSocket socket) noexcept;

    std::expected<void, uv::Error> createContext() noexcept;

    std::expected<void, uv::Error> createTLSObjects() noexcept;

    std::expected<void, uv::Error> startTransportRead() noexcept;

    std::expected<void, uv::Error> startHandshake() noexcept;

    std::expected<void, uv::Error> driveHandshake() noexcept;

    std::expected<void, uv::Error> drainPlaintextReads() noexcept;

    std::expected<void, uv::Error> queueWrite(
        std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept;

    std::expected<void, uv::Error> processPendingPlaintextWrites() noexcept;

    std::expected<void, uv::Error> flushCiphertextBIO() noexcept;

    std::expected<void, uv::Error> processPendingCiphertextWrites() noexcept;

    void onTransportRead(std::expected<std::span<const uint8_t>, uv::Error> result) noexcept;

    void onUnderlyingWriteDone(size_t writeId, std::expected<void, uv::Error> result) noexcept;

    void failPendingWrites(uv::Error error) noexcept;

    std::expected<void, uv::Error> sendCloseNotify() noexcept;

    uv::TCPSocket _socket;
    State _state {State::Uninitialized};

    // TODO: can we allocate these values inside the object itself?
    SSLContextPtr _context {nullptr, &SSL_CTX_free};
    SSLPtr _ssl {nullptr, &SSL_free};
    BIOPtr _readBIO {nullptr, &BIO_free};
    BIOPtr _writeBIO {nullptr, &BIO_free};

    uv::ReadCallback _onRead {};

    bool _readEnabled {false};
    bool _transportReadStarted {false};
    bool _underlyingWriteInFlight {false};
    bool _closeNotifySent {false};

    size_t _nextWriteId {1};
    size_t _pendingPlaintextBytes {0};
    size_t _pendingCiphertextBytes {0};
    size_t _maxPendingBytes {DEFAULT_MAX_PENDING_BYTES};

    std::deque<PendingPlaintextWrite> _pendingPlaintextWrites {};
    std::deque<PendingCiphertextWrite> _pendingCiphertextWrites {};
};

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler
