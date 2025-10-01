cmake_minimum_required(VERSION 3.15)

project(tcpproxy VERSION 1.0 LANGUAGES C)

include(GNUInstallDirs)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_path(RABBITMQ_INCLUDE_DIR amqp.h)
find_library(RABBITMQ_LIBRARY rabbitmq)

if(NOT RABBITMQ_INCLUDE_DIR OR NOT RABBITMQ_LIBRARY)
  message(FATAL_ERROR "librabbitmq not found - please install rabbitmq-c development files")
endif()

find_path(PCRE2_INCLUDE_DIR pcre2.h)
find_library(PCRE2_LIBRARY pcre2-8)

if(NOT PCRE2_INCLUDE_DIR OR NOT PCRE2_LIBRARY)
  message(FATAL_ERROR "PCRE2 (8-bit) not found - please install libpcre2-8 development files")
endif()

add_executable(tcpproxy
  tcpproxy.c
  observer.c
  tracker_parser.c
)

add_executable(tracker_parser_daemon
  tracker_parser_daemon.c
)

target_compile_definitions(tcpproxy PRIVATE PRODUCT="tcpproxy" VERSION="1.0")

target_include_directories(tcpproxy PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${RABBITMQ_INCLUDE_DIR})
target_include_directories(tracker_parser_daemon PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${RABBITMQ_INCLUDE_DIR} ${PCRE2_INCLUDE_DIR})

target_link_libraries(tcpproxy PRIVATE ${RABBITMQ_LIBRARY})
target_link_libraries(tracker_parser_daemon PRIVATE ${RABBITMQ_LIBRARY} ${PCRE2_LIBRARY})

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(tcpproxy PRIVATE -Wall)
  target_compile_options(tracker_parser_daemon PRIVATE -Wall)
endif()

install(TARGETS tcpproxy RUNTIME DESTINATION ${CMAKE_INSTALL_SBINDIR})
install(TARGETS tracker_parser_daemon RUNTIME DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/tcpproxy)
install(PROGRAMS scripts/cat_location_daemon.py DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/tcpproxy)
install(FILES scripts/__init__.py DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/tcpproxy)
install(FILES tcpproxy.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES Locations.kml DESTINATION ${CMAKE_INSTALL_SYSCONFDIR} RENAME Locations.kml.sample)
install(FILES cat_location.service DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system RENAME cat_location.service.sample)
install(FILES tracker_parser_daemon.service DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system RENAME tracker_parser_daemon.service.sample)

find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
  add_custom_target(proxy-tests
    COMMAND ${CMAKE_COMMAND} -E env TCPPROXY_BIN=$<TARGET_FILE:tcpproxy>
            ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/run_proxy_tests.py
    DEPENDS tcpproxy tracker_parser_daemon
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running proxy end-to-end tests"
  )
  add_custom_target(amqp-integration
    COMMAND ${CMAKE_COMMAND} -E env TCPPROXY_BIN=$<TARGET_FILE:tcpproxy>
            TCPPROXY_TRACKER_PARSER=$<TARGET_FILE:tracker_parser_daemon>
            ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/run_amqp_integration.py
    DEPENDS tcpproxy tracker_parser_daemon
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running AMQP integration test (requires podman)"
  )
endif()
