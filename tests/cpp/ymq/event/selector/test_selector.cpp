#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "scaler/ymq/event/event_type.h"
#include "scaler/ymq/event/selector/selector.h"

using namespace scaler::ymq::event;
using namespace scaler::ymq::event::selector;

#ifdef __linux__
#include "scaler/ymq/event/selector/epoll_selector.h"
using SelectorImpl = EpollSelector;
#endif  // __linux__

#ifdef __APPLE__
#include "scaler/ymq/event/selector/kqueue_selector.h"
using SelectorImpl = KQueueSelector;
#endif  // __APPLE__

class SelectorTest: public ::testing::Test {
protected:
    void SetUp() override { selector = new SelectorImpl(); }

    void TearDown() override { delete selector; }

    SelectorImpl* selector;
};

TEST_F(SelectorTest, Select)
{
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);
    fcntl(pipeFds[0], F_SETFL, O_NONBLOCK);
    fcntl(pipeFds[1], F_SETFL, O_NONBLOCK);

    int pipeRd = pipeFds[0];
    int pipeWr = pipeFds[1];

    selector->add(pipeRd, static_cast<EventType>(EventType::Read | EventType::Close));

    constexpr char str[] = "Hello!";

    // Triggers a read event
    {
        write(pipeWr, str, sizeof(str) - 1);

        auto events = selector->select();

        EXPECT_EQ(events.size(), 1);
        EXPECT_EQ(events[0].handle, pipeRd);
        EXPECT_EQ(events[0].events, EventType::Read);
    }

    // Does not repeat the read event
    {
        auto events = selector->select(std::chrono::milliseconds::zero());
        EXPECT_EQ(events.size(), 0);
    }

    // Trigger read + close events
    {
        write(pipeWr, str, sizeof(str) - 1);
        close(pipeWr);

        auto events = selector->select();

        EXPECT_EQ(events.size(), 1);
        EXPECT_EQ(events[0].handle, pipeRd);
        EXPECT_EQ(events[0].events, static_cast<EventType>(EventType::Read | EventType::Close));
    }

    // Remove the FD
    {
        selector->remove(pipeRd);
        close(pipeRd);

        auto events = selector->select(std::chrono::milliseconds(10));

        EXPECT_TRUE(events.empty());
    }
}

TEST_F(SelectorTest, SelectTimeout)
{
    // select()'s timeout should be at least 10% precise
    {
        constexpr std::chrono::milliseconds timeout {100};

        auto start    = std::chrono::steady_clock::now();
        auto events   = selector->select(timeout);
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

        EXPECT_EQ(events.size(), 0);
        EXPECT_NEAR(duration.count(), timeout.count(), timeout.count() * 0.10);
    }

    // select()'s with a zero timeout should return immediately
    {
        auto start    = std::chrono::steady_clock::now();
        auto events   = selector->select(std::chrono::milliseconds::zero());
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

        EXPECT_EQ(events.size(), 0);
        EXPECT_NEAR(duration.count(), 0, 5);  // 0 ms +/- 5 ms
    }
}
