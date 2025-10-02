#pragma once

// Python
#include <stdexcept>
#define PY_SSIZE_T_CLEAN
#include <Python.h>

// C
#include <sys/eventfd.h>
#include <sys/poll.h>

// First-party
#include "scaler/io/ymq/common.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

class Waiter {
public:
    Waiter(YMQState* ymqState): _waiter(new int, &destroyEFD), _ymqState(ymqState)
    {
        auto fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (fd < 0)
            throw std::runtime_error("failed to create eventfd");

        *_waiter = fd;
    }

    Waiter(const Waiter& other): _waiter(other._waiter), _ymqState(other._ymqState) {}
    Waiter(Waiter&& other) noexcept: _waiter(std::move(other._waiter)), _ymqState(other._ymqState)
    {
        other._ymqState = nullptr;  // invalidate the moved-from object
    }

    Waiter& operator=(const Waiter& other)
    {
        if (this == &other)
            return *this;

        this->_waiter   = other._waiter;
        this->_ymqState = other._ymqState;
        return *this;
    }

    Waiter& operator=(Waiter&& other) noexcept
    {
        if (this == &other)
            return *this;

        this->_waiter   = std::move(other._waiter);
        this->_ymqState = other._ymqState;
        other._ymqState = nullptr;  // invalidate the moved-from object
        return *this;
    }

    void signal()
    {
        if (eventfd_write(*_waiter, 1) < 0) {
            throw std::runtime_error("Failed to signal waiter: " + std::string(std::strerror(errno)));
        }
    }

    // Block until signaled through `signal()` and return `true`.
    //
    // If interrupted by an OS signal and an exception is raised by a Python signal handler, returns `false`.
    bool wait()
    {
        pollfd pfds[2] = {
            {
                .fd      = *_waiter,
                .events  = POLLIN,
                .revents = 0,
            },
            {
                .fd      = _ymqState->signalWakeupFDWr,
                .events  = POLLIN,
                .revents = 0,
            }};

        for (;;) {
            int ready = poll(pfds, 2, -1);
            if (ready < 0) {
                if (errno == EINTR)
                    continue;
                throw std::runtime_error("poll failed");
            }

            if (pfds[0].revents & POLLIN)
                return true;  // signal() called

            if (pfds[1].revents & POLLIN) {
                // Interrupted by an OS signal. Run the Python signal handlers and wait again.
                PyEval_RestoreThread(_ymqState->threadState);
                if (PyErr_CheckSignals() == -1) {
                    PyErr_SetString(
                        *_ymqState->PyInterruptedExceptionType,
                        "A synchronous YMQ operation was interrupted by a signal handler exception");
                    _ymqState->threadState = PyEval_SaveThread();
                    return false;
                }
                _ymqState->threadState = PyEval_SaveThread();
            }
        }
    }

private:
    std::shared_ptr<int> _waiter;
    YMQState* _ymqState;

    static void destroyEFD(int* fd)
    {
        if (!fd)
            return;

        close(*fd);
        delete fd;
    }
};
