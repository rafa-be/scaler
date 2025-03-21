#pragma once

// Third-party
#include "third_party/concurrentqueue.h"

// Common
#include "common.hpp"

using moodycamel::BlockingConcurrentQueue;

// --- declarations ---
struct BlockingIntraProcessConnector;

// First-party
#include "common.hpp"
#include "session.hpp"

void blocking_intra_process_init(Session *session, BlockingIntraProcessConnector *connector, uint8_t *identity, size_t len);
void blocking_intra_process_bind(struct BlockingIntraProcessConnector *connector, const char *addr);
void blocking_intra_process_connect(struct BlockingIntraProcessConnector *connector, const char *addr);
void blocking_intra_process_send(struct BlockingIntraProcessConnector *connector, uint8_t *data, size_t len);
void blocking_intra_process_recv_sync(struct BlockingIntraProcessConnector *connector, struct Message *msg);
void blocking_intra_process_recv_async(void *future, struct BlockingIntraProcessConnector *connector);
void blocking_intra_process_destroy(struct BlockingIntraProcessConnector *connector);


struct BlockingIntraProcessConnector
{
    Session *session;
    ThreadContext *thread;

    std::string name;

    BlockingConcurrentQueue<Message> message_queue;
    BlockingConcurrentQueue<Message> connect_queue;
};
