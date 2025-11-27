add_test_executable(test_ymq SOURCES test_ymq.cpp)

target_sources(test_ymq PRIVATE
	event/selector/test_selector.cpp
	event/test_event_selector_loop.cpp

	common/testing.h
	common/utils.h
	common/utils.cpp

	net/socket.h
)

if(LINUX OR APPLE)
	target_sources(test_ymq PRIVATE
		net/socket_linux.cpp)
elseif(WIN32)
	target_sources(test_ymq PRIVATE
		net/socket_windows.cpp)
endif()
