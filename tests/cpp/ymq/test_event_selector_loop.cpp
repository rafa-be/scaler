#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "scaler/io/ymq/event_selector_loop.h"
#include "scaler/io/ymq/timestamp.h"

using namespace scaler::ymq;

#ifdef __linux__
using EventSelectorLoopImpl = EventSelectorLoop<EpollSelector>;
#endif  // __linux__

#ifdef __APPLE__
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
    loop->executeAt(Timestamp() + delay, [&]() { ++nTimesCalled; });

    ASSERT_EQ(nTimesCalled, 0);

    // Some implementations of select() may return slightly earlier than the provided timeout (e.g. KQueue). So we might
    // have to run routine() multiple times until it processes the scheduled task.
    do {
        loop->routine();
    } while (nTimesCalled == 0);

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

    EXPECT_NEAR(elapsed.count(), delay.count(), delay.count() * 0.10);
    ASSERT_EQ(nTimesCalled, 1);
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
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);
    fcntl(pipeFds[0], F_SETFL, O_NONBLOCK);
    fcntl(pipeFds[1], F_SETFL, O_NONBLOCK);

    int pipeRd = pipeFds[0];
    int pipeWr = pipeFds[1];

    bool readCalled  = false;
    bool closeCalled = false;

    EventManager manager;
    manager.onRead  = [&readCalled]() { readCalled = true; };
    manager.onClose = [&closeCalled]() { closeCalled = true; };

    loop->addFdToLoop(
        pipeRd, static_cast<EventSelectorType>(EventSelectorType::Read | EventSelectorType::Close), std::move(manager));

    constexpr char str[] = "Hello!";

    // Trigger a read event
    {
        write(pipeWr, str, sizeof(str) - 1);

        loop->routine();

        ASSERT_TRUE(readCalled);
        ASSERT_FALSE(closeCalled);

        readCalled = false;
    }

    // Trigger read and close events
    {
        write(pipeWr, str, sizeof(str) - 1);
        close(pipeWr);

        loop->routine();

        ASSERT_TRUE(readCalled);
        ASSERT_TRUE(closeCalled);
    }

    loop->removeFdFromLoop(pipeRd);
    close(pipeRd);
}
