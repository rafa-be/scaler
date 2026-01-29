add_executable(event_loop_thread_benchmark event_loop_thread_benchmark.cpp)
target_link_libraries(event_loop_thread_benchmark PRIVATE
    uv_ymq_objs
    scaler_wrapper_uv
)
