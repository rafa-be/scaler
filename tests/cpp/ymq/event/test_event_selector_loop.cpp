#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "scaler/utility/pipe/pipe.h"
#include "scaler/utility/timestamp.h"
#include "scaler/ymq/event/event_selector_loop.h"
#include "scaler/ymq/event/event_type.h"
#include "scaler/ymq/event_manager.h"

using namespace scaler::utility;
using namespace scaler::utility::pipe;
using namespace scaler::ymq;
using namespace scaler::ymq::event;

#ifdef __linux__
#include "scaler/ymq/event/selector/epoll_selector.h"

using EventSelectorLoopImpl = EventSelectorLoop<EpollSelector>;
#endif  // __linux__

#ifdef __APPLE__
#include "scaler/ymq/event/selector/kqueue_selector.h"

using EventSelectorLoopImpl = EventSelectorLoop<KQueueSelector>;
#endif  // __APPLE__

class EventSelectorLoopTest: public ::testing::Test {
protected:
    void SetUp() override { loop = new EventSelectorLoopImpl(); }

    void TearDown() override { delete loop; }

    EventSelectorLoopImpl* loop;
};

TEST_F(EventSelectorLoopTest, ExecuteSoon)
{
    int nTimesCalled = 0;

    loop->executeSoon([&]() { ++nTimesCalled; });

    ASSERT_EQ(nTimesCalled, 0);

    loop->routine();

    ASSERT_EQ(nTimesCalled, 1);
}

TEST_F(EventSelectorLoopTest, ExecuteSoonThreadSafe)
{
    std::promise<void> threadReadyEvent;
    std::thread thread([&]() {
        threadReadyEvent.set_value();
        loop->routine();
    });

    threadReadyEvent.get_future().wait();

    int nTimesCalled = 0;

    loop->executeSoonThreadSafe([&]() { ++nTimesCalled; });

    thread.join();

    ASSERT_EQ(nTimesCalled, 1);
}

TEST_F(EventSelectorLoopTest, ExecuteAt)
{
    constexpr std::chrono::milliseconds delay {100};

    int nTimesCalled = 0;

    auto start = std::chrono::steady_clock::now();

    // Schedule two tasks in the future.
    loop->executeAt(Timestamp() + (2 * delay), [&]() { ++nTimesCalled; });
    loop->executeAt(Timestamp() + delay, [&]() { ++nTimesCalled; });

    ASSERT_EQ(nTimesCalled, 0);

    // The first task should be executed after ~100ms.
    {
        // Some implementations of select() may return earlier than the provided timeout (e.g. KQueue). So we might have
        // to run routine() multiple times until it processes the scheduled task.
        do {
            loop->routine();
        } while (nTimesCalled == 0);

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

        EXPECT_NEAR(elapsed.count(), delay.count(), delay.count() * 0.10);
        ASSERT_EQ(nTimesCalled, 1);
    }

    // The second task should be executed after ~200ms.
    {
        do {
            loop->routine();
        } while (nTimesCalled == 1);

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

        EXPECT_NEAR(elapsed.count(), 2 * delay.count(), delay.count() * 0.10);
        ASSERT_EQ(nTimesCalled, 2);
    }
}

TEST_F(EventSelectorLoopTest, CancelExecution)
{
    constexpr std::chrono::milliseconds delay {100};

    int nTimesCalled = 0;

    auto task = loop->executeAt(Timestamp() + delay, [&]() { ++nTimesCalled; });
    loop->executeSoon([&]() {});  // we have to queue a dummy function, or routine() will block

    loop->cancelExecution(task);

    loop->routine();

    ASSERT_EQ(nTimesCalled, 0);
}

TEST_F(EventSelectorLoopTest, AddFdToLoop)
{
    Pipe pipe;
    pipe.reader.setNonBlocking();

    bool readCalled  = false;
    bool closeCalled = false;

    EventManager manager;
    manager.onRead  = [&readCalled]() { readCalled = true; };
    manager.onClose = [&closeCalled]() { closeCalled = true; };

    loop->addFdToLoop(pipe.reader.fd(), static_cast<EventType>(EventType::Read | EventType::Close), std::move(manager));

    constexpr std::array<uint8_t, 3> message = {':', '-', ')'};

    // Trigger a read event
    {
        pipe.writer.writeAll(message);

        loop->routine();

        ASSERT_TRUE(readCalled);
        ASSERT_FALSE(closeCalled);

        readCalled = false;
    }

    // Trigger read and close events
    {
        pipe.writer.writeAll(message);
        {
            auto writer = std::move(pipe.writer);  // forces the early closing of the writer
        }

        loop->routine();

        ASSERT_TRUE(readCalled);
        ASSERT_TRUE(closeCalled);
    }

    loop->removeFdFromLoop(pipe.reader.fd());
}