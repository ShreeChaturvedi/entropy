# Standalone smoke-test project. NOT part of the Entropy build; it is configured
# separately against an installed prefix to prove the exported package config
# (find_package(entropy) + entropy::entropy) actually links.
cmake_minimum_required(VERSION 3.20)

project(entropy_consumer_smoke LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(entropy CONFIG REQUIRED)

add_executable(consumer_smoke main.cpp)
target_link_libraries(consumer_smoke PRIVATE entropy::entropy)
