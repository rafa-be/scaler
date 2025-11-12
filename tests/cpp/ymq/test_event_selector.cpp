#include <fcntl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "scaler/io/ymq/event_selector.h"

using namespace scaler::ymq;

#ifdef __linux__
using EventSelectorImpl = EpollSelector;
#endif  // __linux__

#ifdef __APPLE__
using EventSelectorImpl = KQueueSelector;
#endif  // __APPLE__

class EventSelectorTest: public ::testing::Test {
protected:
    void SetUp() override { selector = new EventSelectorImpl(); }

    void TearDown() override { delete selector; }

    EventSelectorImpl* selector;
};

TEST_F(EventSelectorTest, Select)
{
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);
    fcntl(pipeFds[0], F_SETFL, O_NONBLOCK);
    fcntl(pipeFds[1], F_SETFL, O_NONBLOCK);

    int pipeRd = pipeFds[0];
    int pipeWr = pipeFds[1];

    selector->add(pipeRd, static_cast<EventSelectorType>(EventSelectorType::Read | EventSelectorType::Close));

    constexpr char str[] = "Hello!";
    write(pipeWr, str, sizeof(str) - 1);

    auto events = selector->select();

    EXPECT_EQ(events.size(), 1);
    EXPECT_EQ(events[0].identifier, pipeRd);
    EXPECT_EQ(events[0].types, EventSelectorType::Read);

    write(pipeWr, str, sizeof(str) - 1);
    close(pipeWr);

    events = selector->select();

    EXPECT_EQ(events.size(), 1);
    EXPECT_EQ(events[0].types, static_cast<EventSelectorType>(EventSelectorType::Read | EventSelectorType::Close));
    EXPECT_EQ(events[0].identifier, pipeRd);

    selector->remove(pipeRd);
    close(pipeRd);

    events = selector->select(std::chrono::milliseconds(10));

    EXPECT_TRUE(events.empty());
}

TEST_F(EventSelectorTest, SelectTimeout)
{
    constexpr std::chrono::milliseconds timeout {100};

    auto start    = std::chrono::steady_clock::now();
    auto events   = selector->select(timeout);
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

    EXPECT_EQ(events.size(), 0);

    // select()'s timeout should be at least 10% precise
    EXPECT_NEAR(duration.count(), timeout.count(), timeout.count() * 0.10);
}
