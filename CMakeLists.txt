cmake_minimum_required(VERSION 3.20)
project(httpkafka)

find_package(Qt6 REQUIRED COMPONENTS Core Network Protobuf StateMachine)
qt_standard_project_setup()

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_COLOR_MAKEFILE OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

qt_add_protobuf(protofiles
  PROTO_FILES
  proto/position_report.proto
)
target_include_directories(protofiles PRIVATE ${CMAKE_BINARY_DIR})


add_executable(httpkafka
  src/main.cpp

  src/kafka_rest_api.h
  src/kafka_rest_api.cpp

  src/proxy_consumer.h
  src/proxy_consumer.cpp

  src/proxy_producer.h
  src/proxy_producer.cpp
  
)


target_include_directories(httpkafka PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(httpkafka PRIVATE protofiles Qt6::Core Qt6::Network Qt6::Protobuf Qt6::StateMachine)
