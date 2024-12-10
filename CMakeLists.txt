cmake_minimum_required(VERSION 3.20)
project(httpkafka)

find_package(Qt6 REQUIRED COMPONENTS Core Network StateMachine)
qt_standard_project_setup()

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_COLOR_MAKEFILE OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(httpkafka
  src/kafka_rest_api.cpp
  src/proxy_consumer.cpp
  src/proxy_producer.cpp

  inc/kafka_rest_api.h
  inc/proxy_consumer.h
  inc/proxy_producer.h
)

set_target_properties(httpkafka PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(httpkafka PRIVATE inc)
target_link_libraries(httpkafka PRIVATE Qt6::Core Qt6::Network Qt6::StateMachine)

install(TARGETS httpkafka  LIBRARY DESTINATION lib)
install(DIRECTORY inc/ DESTINATION include/httpkafka)

