cmake_minimum_required(VERSION 3.16)

project(protectdb_app LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Pull NuRaft as a library dependency without building its examples/tests.
if(NOT TARGET NuRaft::static_lib)
    set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
    add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/../NuRaft ${CMAKE_CURRENT_BINARY_DIR}/nuraft-build)
endif()

find_path(LIBPQ_INCLUDE_DIR
          NAMES libpq-fe.h
          HINTS
            ${CMAKE_CURRENT_LIST_DIR}/../src/interfaces/libpq
            /usr/include
            /usr/local/include
            /usr/include/postgresql
            /usr/local/include/postgresql)

find_path(POSTGRES_INCLUDE_DIR
          NAMES postgres_ext.h
          HINTS
            ${CMAKE_CURRENT_LIST_DIR}/../src/include
            /usr/include/postgresql
            /usr/local/include/postgresql
            /usr/include
            /usr/local/include)

find_library(LIBPQ_LIBRARY
             NAMES pq libpq
             HINTS
               ${CMAKE_CURRENT_LIST_DIR}/../src/interfaces/libpq
               /usr/lib
               /usr/local/lib
               /usr/lib/x86_64-linux-gnu
               /usr/local/lib/x86_64-linux-gnu)

find_path(RDKAFKA_INCLUDE_DIR
          NAMES librdkafka/rdkafka.h
          HINTS
            $ENV{HOME}/Desktop/rdkafka_local/include
            /usr/include
            /usr/local/include)

find_library(RDKAFKA_LIBRARY
             NAMES rdkafka
             HINTS
               $ENV{HOME}/Desktop/rdkafka_local/lib
               /usr/lib
               /usr/local/lib
               /usr/lib/x86_64-linux-gnu
               /usr/local/lib/x86_64-linux-gnu)

option(KAFKA_OPTIONAL "Allow building without librdkafka (disables Kafka result publishing)" OFF)

if(NOT RDKAFKA_INCLUDE_DIR OR NOT RDKAFKA_LIBRARY)
    if(KAFKA_OPTIONAL)
        message(WARNING "librdkafka not found — building without Kafka support (KAFKA_OPTIONAL=ON).")
        set(RDKAFKA_INCLUDE_DIR "")
        set(RDKAFKA_LIBRARY "")
        set(ARIABC_PG_KAFKA_LIBS "")
        set(ARIABC_HAVE_KAFKA OFF)
    else()
        message(FATAL_ERROR "librdkafka is required. Install librdkafka development headers and library, or pass -DKAFKA_OPTIONAL=ON to build without Kafka.")
    endif()
else()
    set(ARIABC_HAVE_KAFKA ON)
    set(ARIABC_PG_KAFKA_LIBS ${RDKAFKA_LIBRARY})
endif()

set(ARIABC_COMMON_SRC
    src/logger.cc
    src/in_memory_log_store.cxx)

set(ARIABC_COMMON_INC
    ${CMAKE_CURRENT_LIST_DIR}/src
    ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/include
    ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/include/libnuraft
    ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/src)

# NuRaft examples traditionally build with standalone ASIO unless BOOST_ASIO=ON.
set(ARIABC_TRANSPORT_DEFS ASIO_STANDALONE)
if(BOOST_ASIO)
    set(ARIABC_TRANSPORT_DEFS USE_BOOST_ASIO)
endif()

if(ARIABC_HAVE_KAFKA)
    add_compile_definitions(ARIABC_HAVE_RDKAFKA)
endif()

if(LIBPQ_INCLUDE_DIR AND LIBPQ_LIBRARY)
    add_executable(protectdb_raft_db_coordinator
                   src/protectdb_raft_db_coordinator.cxx
                   src/pg_state_machine.cxx
                   src/pg_executor.cxx
                   src/kafka_console.cxx
                   src/wire_protocol.cxx
                   src/raft_storage_common.cxx
                   src/durable_state_mgr.cxx
                   src/durable_log_store.cxx
                   ${ARIABC_COMMON_SRC})

    target_include_directories(protectdb_raft_db_coordinator
                              PRIVATE
                                ${ARIABC_COMMON_INC}
                                ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include
                                ${LIBPQ_INCLUDE_DIR}
                                ${POSTGRES_INCLUDE_DIR}
                                ${RDKAFKA_INCLUDE_DIR})

    target_compile_definitions(protectdb_raft_db_coordinator PRIVATE ${ARIABC_TRANSPORT_DEFS})

    target_link_libraries(protectdb_raft_db_coordinator
                          NuRaft::static_lib
                          ${LIBPQ_LIBRARY}
                          ${ARIABC_PG_KAFKA_LIBS}
                          z
                          dl)
else()
    message(WARNING "libpq not found (libpq-fe.h / -lpq). Skipping protectdb_raft_db_coordinator build.")
endif()

add_executable(protectdb_queryClient_resultAggregator
               src/protectdb_queryClient_resultAggregator.cxx
               src/async_cluster_submitter.cxx
               src/kafka_console.cxx
               src/wire_protocol_async.cxx
               src/wire_protocol.cxx)

target_include_directories(protectdb_queryClient_resultAggregator
                          PRIVATE
                            ${ARIABC_COMMON_INC}
                            ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include
                            ${RDKAFKA_INCLUDE_DIR})

target_compile_definitions(protectdb_queryClient_resultAggregator PRIVATE ${ARIABC_TRANSPORT_DEFS})

target_link_libraries(protectdb_queryClient_resultAggregator
                      NuRaft::static_lib
                      ${ARIABC_PG_KAFKA_LIBS}
                      z
                      dl)

if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/src/wire_protocol_test.cxx")
    add_executable(ariabc_pg_wire_protocol_test
                   src/wire_protocol_test.cxx
                   src/wire_protocol.cxx
                   src/wire_protocol_async.cxx)

    target_include_directories(ariabc_pg_wire_protocol_test
                              PRIVATE
                                ${ARIABC_COMMON_INC}
                                ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include)

    target_compile_definitions(ariabc_pg_wire_protocol_test PRIVATE ${ARIABC_TRANSPORT_DEFS})

    target_link_libraries(ariabc_pg_wire_protocol_test
                          NuRaft::static_lib
                          z
                          dl)
else()
    message(STATUS "Skipping ariabc_pg_wire_protocol_test: src/wire_protocol_test.cxx not present in this checkout")
endif()

# Durable Storage Unit Tests
add_executable(durable_state_mgr_test
               tests/durable_state_mgr_test.cxx
               src/raft_storage_common.cxx
               src/durable_state_mgr.cxx
               src/durable_log_store.cxx)

target_include_directories(durable_state_mgr_test
                           PRIVATE
                             ${ARIABC_COMMON_INC}
                             ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include)

target_compile_definitions(durable_state_mgr_test PRIVATE ${ARIABC_TRANSPORT_DEFS})

target_link_libraries(durable_state_mgr_test
                      NuRaft::static_lib
                      z
                      dl)

add_executable(durable_log_store_test
               tests/durable_log_store_test.cxx
               src/raft_storage_common.cxx
               src/durable_log_store.cxx)

target_include_directories(durable_log_store_test
                           PRIVATE
                             ${ARIABC_COMMON_INC}
                             ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include)

target_compile_definitions(durable_log_store_test PRIVATE ${ARIABC_TRANSPORT_DEFS})

target_link_libraries(durable_log_store_test
                      NuRaft::static_lib
                      z
                      dl)

add_executable(durable_raft_cluster_smoke_test
               tests/durable_raft_cluster_smoke_test.cxx
               src/raft_storage_common.cxx
               src/durable_state_mgr.cxx
               src/durable_log_store.cxx
               src/logger.cc)

target_include_directories(durable_raft_cluster_smoke_test
                            PRIVATE
                              ${ARIABC_COMMON_INC}
                              ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include)

target_compile_definitions(durable_raft_cluster_smoke_test PRIVATE ${ARIABC_TRANSPORT_DEFS})

target_link_libraries(durable_raft_cluster_smoke_test
                       NuRaft::static_lib
                       z
                       dl)

add_executable(durable_raft_external_sigkill_test
               tests/durable_raft_external_sigkill_test.cxx
               src/raft_storage_common.cxx
               src/durable_state_mgr.cxx
               src/durable_log_store.cxx
               src/logger.cc)

target_include_directories(durable_raft_external_sigkill_test
                            PRIVATE
                              ${ARIABC_COMMON_INC}
                              ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include)

target_compile_definitions(durable_raft_external_sigkill_test PRIVATE ${ARIABC_TRANSPORT_DEFS})

target_link_libraries(durable_raft_external_sigkill_test
                       NuRaft::static_lib
                       z
                       dl)

add_executable(b4_codec_sig_test
               tests/b4_codec_sig_test.cxx
               src/pg_executor.cxx
               src/protectdb_queryClient_resultAggregator.cxx
               src/async_cluster_submitter.cxx
               src/kafka_console.cxx
               src/wire_protocol.cxx
               src/wire_protocol_async.cxx
               src/raft_storage_common.cxx
               src/durable_state_mgr.cxx
               src/durable_log_store.cxx
               src/logger.cc
               src/in_memory_log_store.cxx)

target_include_directories(b4_codec_sig_test
                            PRIVATE
                              ${ARIABC_COMMON_INC}
                              ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include
                              ${LIBPQ_INCLUDE_DIR}
                              ${POSTGRES_INCLUDE_DIR}
                              ${RDKAFKA_INCLUDE_DIR})

target_compile_definitions(b4_codec_sig_test PRIVATE ${ARIABC_TRANSPORT_DEFS} BUILDING_UNIT_TESTS)

target_link_libraries(b4_codec_sig_test
                      NuRaft::static_lib
                      ${LIBPQ_LIBRARY}
                      ${ARIABC_PG_KAFKA_LIBS}
                      z
                      dl)

enable_testing()
add_test(NAME durable_state_mgr_test COMMAND durable_state_mgr_test)
add_test(NAME durable_log_store_test COMMAND durable_log_store_test)
add_test(NAME durable_raft_cluster_smoke_test COMMAND durable_raft_cluster_smoke_test)
add_test(NAME durable_raft_external_sigkill_test COMMAND durable_raft_external_sigkill_test)
add_test(NAME b4_codec_sig_test COMMAND b4_codec_sig_test)

add_executable(pg_state_machine_test
               tests/pg_state_machine_test.cxx
               src/pg_state_machine.cxx
               src/pg_executor.cxx
               src/kafka_console.cxx
               src/wire_protocol.cxx)

target_include_directories(pg_state_machine_test
                            PRIVATE
                              ${ARIABC_COMMON_INC}
                              ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include
                              ${LIBPQ_INCLUDE_DIR}
                              ${POSTGRES_INCLUDE_DIR}
                              ${RDKAFKA_INCLUDE_DIR})

target_compile_definitions(pg_state_machine_test PRIVATE ${ARIABC_TRANSPORT_DEFS} BUILDING_UNIT_TESTS)

target_link_libraries(pg_state_machine_test
                      NuRaft::static_lib
                      ${LIBPQ_LIBRARY}
                      ${ARIABC_PG_KAFKA_LIBS}
                      z
                      dl)
add_test(NAME pg_state_machine_test COMMAND pg_state_machine_test)

# P0-B: NuRaft durable apply contract test.
# Proves the restart delivery guarantee that AriaBC relies on:
#   - committed entries re-delivered after zero-prefix restart
#   - uncommitted suffix never delivered
#   - no-op/config entries advance prefix without application data
#   - multi-entry ordered delivery maintained on restart
#   - applied prefix cannot jump over a gap
add_executable(durable_raft_pg_apply_contract_test
               tests/durable_raft_pg_apply_contract_test.cxx
               src/pg_state_machine.cxx
               src/pg_executor.cxx
               src/kafka_console.cxx
               src/wire_protocol.cxx
               src/raft_storage_common.cxx
               src/durable_state_mgr.cxx
               src/durable_log_store.cxx
               src/logger.cc
               src/in_memory_log_store.cxx)

target_include_directories(durable_raft_pg_apply_contract_test
                            PRIVATE
                              ${ARIABC_COMMON_INC}
                              ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include
                              ${LIBPQ_INCLUDE_DIR}
                              ${POSTGRES_INCLUDE_DIR}
                              ${RDKAFKA_INCLUDE_DIR})

target_compile_definitions(durable_raft_pg_apply_contract_test PRIVATE ${ARIABC_TRANSPORT_DEFS} BUILDING_UNIT_TESTS)

target_link_libraries(durable_raft_pg_apply_contract_test
                      NuRaft::static_lib
                      ${LIBPQ_LIBRARY}
                      ${ARIABC_PG_KAFKA_LIBS}
                      z
                      dl)

add_test(NAME durable_raft_pg_apply_contract_test COMMAND durable_raft_pg_apply_contract_test)

# ============================================================================
# durable_raft_restart_contract_test
# P0-B (Real): proves NuRaft re-delivers committed entries after restart
# using actual durable log storage.
# ============================================================================
add_executable(durable_raft_restart_contract_test
               tests/durable_raft_restart_contract_test.cxx
               src/raft_storage_common.cxx
               src/durable_state_mgr.cxx
               src/durable_log_store.cxx
               src/logger.cc
               src/in_memory_log_store.cxx)

target_include_directories(durable_raft_restart_contract_test
                            PRIVATE
                              ${ARIABC_COMMON_INC}
                              ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include)

target_compile_definitions(durable_raft_restart_contract_test PRIVATE ${ARIABC_TRANSPORT_DEFS})

target_link_libraries(durable_raft_restart_contract_test
                      NuRaft::static_lib
                      z
                      dl)

add_test(NAME durable_raft_restart_contract_test COMMAND durable_raft_restart_contract_test)
