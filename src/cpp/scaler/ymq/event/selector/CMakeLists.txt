target_sources(ymq_objs PRIVATE
    selector.h
)

if(LINUX)
    target_sources(ymq_objs PRIVATE
        epoll_selector.cpp
        epoll_selector.h
    )
endif()

if(APPLE OR BSD)
    target_sources(ymq_objs PRIVATE
        kqueue_selector.h
        kqueue_selector.cpp
    )
endif()