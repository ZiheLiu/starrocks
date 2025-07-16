set(EXEC_FILES
        ./exec/join_hash_map_test.cpp
        )

if (${USE_AVX512})
    list(APPEND EXEC_FILES ./simd/delta_decode_test.cpp)
endif()


#if ("${WITH_STARCACHE}" STREQUAL "ON")
#    list(APPEND EXEC_FILES ./cache/disk_space_monitor_test.cpp)
#    list(APPEND EXEC_FILES ./cache/lrucache_engine_test.cpp)
#    list(APPEND EXEC_FILES ./cache/starcache_engine_test.cpp)
#    list(APPEND EXEC_FILES ./cache/block_cache/block_cache_test.cpp)
#    list(APPEND EXEC_FILES ./io/cache_input_stream_test.cpp)
#endif ()
#
#if ("${USE_STAROS}" STREQUAL "ON")
#    list(APPEND EXEC_FILES ./fs/fs_starlet_test.cpp)
#    list(APPEND EXEC_FILES ./storage/lake/local_pk_index_manager_test.cpp)
#endif ()

#if (USE_AVX2)
#    set(EXEC_FILES ${EXEC_FILES} ./column/avx_numeric_column_test.cpp)
#endif ()

add_library(starrocks_test_objs OBJECT ${EXEC_FILES})
SET_TARGET_PROPERTIES(starrocks_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control -g1")
add_executable(starrocks_test test_main.cpp $<TARGET_OBJECTS:starrocks_test_objs>)

TARGET_LINK_LIBRARIES(starrocks_test ${TEST_LINK_LIBS})

# =================================================
# test cases must be compiled as a standalone binary
ADD_BE_TEST(storage/task/engine_storage_migration_task_test)
ADD_BE_TEST(storage/lake/compaction_task_test)

# you can simply add test case as single binary without adding `main`
# ADD_BE_TEST(exec/sorting_test)
