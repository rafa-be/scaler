if(LINUX)
    add_test_executable(test_ymq test_ymq.cpp)
endif()

if(LINUX OR APPLE)
    add_test_executable(test_event_selector test_event_selector.cpp)
    add_test_executable(test_event_selector_loop test_event_selector_loop.cpp)
endif()

add_test_executable(test_ymq_logging test_ymq_logging.cpp)
