// this file contains the tests for the C++ interface of YMQ
// each test case is comprised of at least one client and one server, and possibly a middleman
// the clients and servers used in these tests are defined in the first part of this file
//
// the men in the middle (mitm) are implemented using Python and are found in py_mitm/
// in that directory, `main.py` is the entrypoint and framework for all the mitm,
// and the individual mitm implementations are found in their respective files
//
// the test cases are at the bottom of this file, after the clients and servers
// the documentation for each case is found on the TEST() definition

#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/ip.h>
#include <semaphore.h>
#include <sys/mman.h>

#include <cassert>
#include <cstdint>
#include <future>
#include <limits>
#include <string>
#include <thread>

#include "common.h"
#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/simple_interface.h"
#include "tests/cpp/ymq/common.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

// ━━━━━━━━━━━━━━━━━━━
//  clients and servers
// ━━━━━━━━━━━━━━━━━━━

TestResult basic_server_ymq(std::string host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, format_address(host, port));
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult basic_client_ymq(std::string host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, format_address(host, port));
    auto result = syncSendMessage(socket, {.address = Bytes("server"), .payload = Bytes("yi er san si wu liu")});

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult basic_server_raw(std::string host, uint16_t port)
{
    TcpSocket socket;

    socket.bind(host.c_str(), port);
    socket.listen();
    auto [client, _] = socket.accept();
    client.write_message("server");
    auto client_identity = client.read_message();
    RETURN_FAILURE_IF_FALSE(client_identity == "client");
    auto msg = client.read_message();
    RETURN_FAILURE_IF_FALSE(msg == "yi er san si wu liu");

    return TestResult::Success;
}

TestResult basic_client_raw(std::string host, uint16_t port)
{
    TcpSocket socket;

    socket.connect(host.c_str(), port);
    socket.write_message("client");
    auto server_identity = socket.read_message();
    RETURN_FAILURE_IF_FALSE(server_identity == "server");
    socket.write_message("yi er san si wu liu");

    return TestResult::Success;
}

TestResult server_receives_big_message(std::string host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, format_address(host, port));
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.len() == 500'000'000);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult client_sends_big_message(std::string host, uint16_t port)
{
    TcpSocket socket;

    socket.connect(host.c_str(), port);
    socket.write_message("client");
    auto remote_identity = socket.read_message();
    RETURN_FAILURE_IF_FALSE(remote_identity == "server");
    std::string msg(500'000'000, '.');
    socket.write_message(msg);

    return TestResult::Success;
}

TestResult reconnect_server_main(std::string host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, format_address(host, port));
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "sync");

    auto error = syncSendMessage(socket, {.address = Bytes("client"), .payload = Bytes("acknowledge")});
    RETURN_FAILURE_IF_FALSE(!error);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult reconnect_client_main(std::string host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, format_address(host, port));

    // create the recv future in advance, this remains active between reconnects
    auto future = futureRecvMessage(socket);

    // send "sync" and wait for "acknowledge" in a loop
    // the mitm will send a RST after the first "sync"
    // the "sync" message will be lost, but YMQ should automatically reconnect
    // therefore the next "sync" message should succeed
    for (size_t i = 0; i < 10; i++) {
        auto error = syncSendMessage(socket, {.address = Bytes("server"), .payload = Bytes("sync")});
        RETURN_FAILURE_IF_FALSE(!error);

        auto result = future.wait_for(1s);
        if (result == std::future_status::ready) {
            auto msg = future.get();
            RETURN_FAILURE_IF_FALSE(msg.has_value());
            RETURN_FAILURE_IF_FALSE(msg->payload.as_string() == "acknowledge");
            context.removeIOSocket(socket);

            return TestResult::Success;
        } else if (result == std::future_status::timeout) {
            // timeout, try again
            continue;
        } else {
            std::cerr << "future status error\n";
            return TestResult::Failure;
        }
    }

    std::cerr << "failed to reconnect after 10 attempts\n";
    return TestResult::Failure;
}

TestResult client_simulated_slow_network(const char* host, uint16_t port)
{
    TcpSocket socket;

    socket.connect(host, port);
    socket.write_message("client");
    auto remote_identity = socket.read_message();
    RETURN_FAILURE_IF_FALSE(remote_identity == "server");

    std::string message = "yi er san si wu liu";
    uint64_t header     = message.length();

    socket.write_all((char*)&header, 4);
    std::this_thread::sleep_for(2s);
    socket.write_all((char*)&header + 4, 4);
    std::this_thread::sleep_for(3s);
    socket.write_all(message.data(), header / 2);
    std::this_thread::sleep_for(2s);
    socket.write_all(message.data() + header / 2, header - header / 2);

    return TestResult::Success;
}

TestResult client_sends_incomplete_identity(const char* host, uint16_t port)
{
    // open a socket, write an incomplete identity and exit
    {
        TcpSocket socket;

        socket.connect(host, port);

        auto server_identity = socket.read_message();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");

        // write incomplete identity and exit
        std::string identity = "client";
        uint64_t header      = identity.length();
        socket.write_all((char*)&header, 8);
        socket.write_all(identity.data(), identity.length() - 2);
    }

    // connect again and try to send a message
    {
        TcpSocket socket;
        socket.connect(host, port);
        auto server_identity = socket.read_message();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");
        socket.write_message("client");
        socket.write_message("yi er san si wu liu");
    }

    return TestResult::Success;
}

TestResult server_receives_huge_header(const char* host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, format_address(host, port));
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult client_sends_huge_header(const char* host, uint16_t port)
{
    // ignore SIGPIPE so that write() returns EPIPE instead of crashing the program
    signal(SIGPIPE, SIG_IGN);

    {
        TcpSocket socket;

        socket.connect(host, port);
        socket.write_message("client");
        auto server_identity = socket.read_message();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");

        // write the huge header
        uint64_t header = std::numeric_limits<uint64_t>::max();
        socket.write_all((char*)&header, 8);

        size_t i = 0;
        for (; i < 10; i++) {
            std::this_thread::sleep_for(1s);

            try {
                socket.write_all("yi er san si wu liu");
            } catch (const std::system_error& e) {
                if (e.code().value() == EPIPE) {
                    std::cout << "writing failed with EPIPE as expected after sending huge header, continuing...\n";
                    break;  // this is expected
                }

                throw;  // rethrow other errors
            }
        }

        if (i == 10) {
            std::cout << "expected EPIPE after sending huge header\n";
            return TestResult::Failure;
        }
    }

    {
        TcpSocket socket;
        socket.connect(host, port);
        socket.write_message("client");
        auto server_identity = socket.read_message();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");
        socket.write_message("yi er san si wu liu");
    }

    return TestResult::Success;
}

TestResult server_receives_empty_messages(const char* host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, format_address(host, port));

    auto result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "");

    auto result2 = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result2.has_value());
    RETURN_FAILURE_IF_FALSE(result2->payload.as_string() == "");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult client_sends_empty_messages(std::string host, uint16_t port)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, format_address(host, port));

    auto error = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes()});
    RETURN_FAILURE_IF_FALSE(!error);

    auto error2 = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes("")});
    RETURN_FAILURE_IF_FALSE(!error2);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult pubsub_subscriber(std::string host, uint16_t port, std::string topic, int differentiator, sem_t* sem)
{
    IOContext context(1);

    auto socket =
        syncCreateSocket(context, IOSocketType::Unicast, std::format("{}_subscriber_{}", topic, differentiator));

    std::this_thread::sleep_for(500ms);

    syncConnectSocket(socket, format_address(host, port));

    std::this_thread::sleep_for(500ms);

    if (sem_post(sem) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to signal semaphore");
    sem_close(sem);

    auto msg = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(msg.has_value());
    RETURN_FAILURE_IF_FALSE(msg->payload.as_string() == "hello topic " + topic);

    context.removeIOSocket(socket);
    return TestResult::Success;
}

// topic: the identifier of the topic, must match what's passed to the subscribers
// sem: a semaphore to synchronize the publisher and subscriber processes
// n: the number of subscribers
TestResult pubsub_publisher(std::string host, uint16_t port, std::string topic, sem_t* sem, int n)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Multicast, "publisher");
    syncBindSocket(socket, format_address(host, port));

    // wait for the subscribers to be ready
    for (int i = 0; i < n; i++)
        if (sem_wait(sem) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to wait on semaphore");
    sem_close(sem);

    // the topic is wrong, so no one should receive this
    auto error = syncSendMessage(
        socket, Message {.address = Bytes(std::format("x{}", topic)), .payload = Bytes("no one should get this")});
    RETURN_FAILURE_IF_FALSE(!error);

    // no one should receive this either
    error = syncSendMessage(
        socket,
        Message {.address = Bytes(std::format("{}x", topic)), .payload = Bytes("no one should get this either")});
    RETURN_FAILURE_IF_FALSE(!error);

    error = syncSendMessage(socket, Message {.address = Bytes(topic), .payload = Bytes("hello topic " + topic)});
    RETURN_FAILURE_IF_FALSE(!error);

    context.removeIOSocket(socket);
    return TestResult::Success;
}

// ━━━━━━━━━━━━━
//   test cases
// ━━━━━━━━━━━━━

// this is a 'basic' test which sends a single message from a client to a server
// in this variant, both the client and server are implemented using YMQ
//
// this case includes a _delay_
// this is a thread sleep that happens after the client sends the message, to delay the close() of the socket
// at the moment, if this delay is missing, YMQ will not shut down correctly
TEST(CcYmqTestSuite, TestBasicYMQClientYMQServer)
{
    auto host = "localhost";
    auto port = 2889;

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result =
        test(10, {[=] { return basic_client_ymq(host, port); }, [=] { return basic_server_ymq(host, port); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// same as above, except YMQs protocol is directly implemented on top of a TCP socket
TEST(CcYmqTestSuite, TestBasicRawClientYMQServer)
{
    auto host = "localhost";
    auto port = 2890;

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result =
        test(10, {[=] { return basic_client_raw(host, port); }, [=] { return basic_server_ymq(host, port); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

TEST(CcYmqTestSuite, TestBasicRawClientRawServer)
{
    auto host = "localhost";
    auto port = 2891;

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result =
        test(10, {[=] { return basic_client_raw(host, port); }, [=] { return basic_server_raw(host, port); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// this is the same as above, except that it has no delay before calling close() on the socket
TEST(CcYmqTestSuite, TestBasicRawClientRawServerNoDelay)
{
    auto host = "localhost";
    auto port = 2892;

    auto result =
        test(10, {[=] { return basic_client_raw(host, port); }, [=] { return basic_server_ymq(host, port); }});
    EXPECT_EQ(result, TestResult::Success);
}

TEST(CcYmqTestSuite, TestBasicDelayYMQClientRawServer)
{
    auto host = "localhost";
    auto port = 2893;

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result =
        test(10, {[=] { return basic_client_ymq(host, port); }, [=] { return basic_server_raw(host, port); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// in this test case, the client sends a large message to the server
// YMQ should be able to handle this without issue
TEST(CcYmqTestSuite, TestClientSendBigMessageToServer)
{
    auto host = "localhost";
    auto port = 2894;

    auto result = test(
        10,
        {[=] { return client_sends_big_message(host, port); },
         [=] { return server_receives_big_message(host, port); }});
    EXPECT_EQ(result, TestResult::Success);
}

// this is the no-op/passthrough man in the middle test
// for this test case we use YMQ on both the client side and the server side
// the client connects to the mitm, and the mitm connects to the server
// when the mitm receives packets from the client, it forwards it to the server without changing it
// and similarly when it receives packets from the server, it forwards them to the client
//
// the mitm is implemented in Python. we pass the name of the test case, which corresponds to the Python filename,
// and a list of arguments, which are: mitm ip, mitm port, remote ip, remote port
// this defines the address of the mitm, and the addresses that can connect to it
// for more, see the python mitm files
TEST(CcYmqTestSuite, TestMitmPassthrough)
{
    auto mitm_ip     = "192.0.2.4";
    auto mitm_port   = 2323;
    auto remote_ip   = "192.0.2.3";
    auto remote_port = 23571;

    // the Python program must be the first and only the first function passed to test()
    // we must also pass `true` as the third argument to ensure that Python is fully started
    // before beginning the test
    auto result = test(
        20,
        {[=] { return run_mitm("passthrough", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return basic_client_ymq(mitm_ip, mitm_port); },
         [=] { return basic_server_ymq(remote_ip, remote_port); }},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

// this test uses the mitm to test the reconnect logic of YMQ by sending RST packets
TEST(CcYmqTestSuite, TestMitmReconnect)
{
    auto mitm_ip     = "192.0.2.4";
    auto mitm_port   = 2525;
    auto remote_ip   = "192.0.2.3";
    auto remote_port = 23575;

    auto result = test(
        10,
        {[=] { return run_mitm("send_rst_to_client", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return reconnect_client_main(mitm_ip, mitm_port); },
         [=] { return reconnect_server_main(remote_ip, remote_port); }},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

// TODO: Make this more reliable, and re-enable it
// in this test, the mitm drops a random % of packets arriving from the client and server
TEST(CcYmqTestSuite, TestMitmRandomlyDropPackets)
{
    auto mitm_ip     = "192.0.2.4";
    auto mitm_port   = 2828;
    auto remote_ip   = "192.0.2.3";
    auto remote_port = 23591;

    auto result = test(
        60,
        {[=] { return run_mitm("randomly_drop_packets", mitm_ip, mitm_port, remote_ip, remote_port, {"0.3"}); },
         [=] { return basic_client_ymq(mitm_ip, mitm_port); },
         [=] { return basic_server_ymq(remote_ip, remote_port); }},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

// in this test the client is sending a message to the server
// but we simulate a slow network connection by sending the message in segmented chunks
TEST(CcYmqTestSuite, TestSlowNetwork)
{
    auto host = "localhost";
    auto port = 2895;

    auto result = test(
        20, {[=] { return client_simulated_slow_network(host, port); }, [=] { return basic_server_ymq(host, port); }});
    EXPECT_EQ(result, TestResult::Success);
}

// TODO: figure out why this test fails in ci sometimes, and re-enable
//
// in this test, a client connects to the YMQ server but only partially sends its identity and then disconnects
// then a new client connection is established, and this one sends a complete identity and message
// YMQ should be able to recover from a poorly-behaved client like this
TEST(CcYmqTestSuite, TestClientSendIncompleteIdentity)
{
    auto host = "localhost";
    auto port = 2896;

    auto result = test(
        20,
        {[=] { return client_sends_incomplete_identity(host, port); }, [=] { return basic_server_ymq(host, port); }});
    EXPECT_EQ(result, TestResult::Success);
}

// TODO: this should pass
// currently YMQ rejects the second connection, saying that the message is too large even when it isn't
//
// in this test, the client sends an unrealistically-large header
// it is important that YMQ checks the header size before allocating memory
// both for resilence against attacks and to guard against errors
TEST(CcYmqTestSuite, TestClientSendHugeHeader)
{
    auto host = "localhost";
    auto port = 2897;

    auto result = test(
        20,
        {[=] { return client_sends_huge_header(host, port); },
         [=] { return server_receives_huge_header(host, port); }});
    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the client sends empty messages to the server
// there are in effect two kinds of empty messages: Bytes() and Bytes("")
// in the former case, the bytes contains a nullptr
// in the latter case, the bytes contains a zero-length allocation
// it's important that the behaviour of YMQ is known for both of these cases
TEST(CcYmqTestSuite, TestClientSendEmptyMessage)
{
    auto host = "localhost";
    auto port = 2898;

    auto result = test(
        20,
        {[=] { return client_sends_empty_messages(host, port); },
         [=] { return server_receives_empty_messages(host, port); }});
    EXPECT_EQ(result, TestResult::Success);
}

// this case tests the publish-subscribe pattern of YMQ
// we create one publisher and two subscribers with a common topic
// the publisher will send two messages to the wrong topic
// none of the subscribers should receive these
// and then the publisher will send a message to the correct topic
// both subscribers should receive this message
TEST(CcYmqTestSuite, TestPubSub)
{
    auto host  = "localhost";
    auto port  = 2900;
    auto topic = "mytopic";

    // allocate a semaphore to synchronize the publisher and subscriber processes
    sem_t* sem =
        static_cast<sem_t*>(mmap(nullptr, sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));

    if (sem == MAP_FAILED)
        throw std::system_error(errno, std::generic_category(), "failed to map shared memory for semaphore");

    if (sem_init(sem, 1, 0) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to initialize semaphore");

    auto result = test(
        20,
        {[=] { return pubsub_publisher(host, port, topic, sem, 2); },
         [=] { return pubsub_subscriber(host, port, topic, 0, sem); },
         [=] { return pubsub_subscriber(host, port, topic, 1, sem); }});

    sem_destroy(sem);
    munmap(sem, sizeof(sem_t));

    EXPECT_EQ(result, TestResult::Success);
}
