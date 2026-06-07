cmake_minimum_required(VERSION 3.18)
project(transcript_ingestor_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(transcript-ingestor src/main.cpp)
target_compile_options(transcript-ingestor PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(transcript-ingestor PRIVATE pthread)
