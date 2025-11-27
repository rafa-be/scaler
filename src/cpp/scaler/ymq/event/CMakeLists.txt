target_sources(ymq_objs PRIVATE
    event_loop.h
    event_selector_loop.h
    event_type.h
)

add_subdirectory(selector)
