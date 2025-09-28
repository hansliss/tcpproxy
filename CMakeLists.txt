cmake_minimum_required(VERSION 3.15)

project(tcpproxy VERSION 1.0 LANGUAGES C)

include(GNUInstallDirs)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_executable(tcpproxy
  tcpproxy.c
  observer.c
  tracker_parser.c
)

target_compile_definitions(tcpproxy PRIVATE PRODUCT="tcpproxy" VERSION="1.0")
target_compile_definitions(tcpproxy PRIVATE DEFAULT_AMQP_HELPER_PATH="${CMAKE_INSTALL_FULL_LIBEXECDIR}/tcpproxy/observer_amqp_publisher.py")

target_include_directories(tcpproxy PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(tcpproxy PRIVATE -Wall)
endif()

install(TARGETS tcpproxy RUNTIME DESTINATION ${CMAKE_INSTALL_SBINDIR})
install(PROGRAMS scripts/observer_amqp_publisher.py DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/tcpproxy)
install(FILES tcpproxy.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)

find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
  add_custom_target(proxy-tests
    COMMAND ${CMAKE_COMMAND} -E env TCPPROXY_BIN=$<TARGET_FILE:tcpproxy>
            ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/run_proxy_tests.py
    DEPENDS tcpproxy
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running proxy end-to-end tests"
  )
  add_custom_target(amqp-integration
    COMMAND ${CMAKE_COMMAND} -E env TCPPROXY_BIN=$<TARGET_FILE:tcpproxy>
            ${Python3_EXECUTABLE} ${PROJECT_SOURCE_DIR}/tests/run_amqp_integration.py
    DEPENDS tcpproxy
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running AMQP integration test (requires podman)"
  )
endif()
