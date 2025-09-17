cmake_minimum_required(VERSION 3.12)

project(async VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# Library
add_library(async SHARED
  source/async.cpp
  source/dispatcher.cpp
)
target_include_directories(async PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(async PRIVATE Threads::Threads)
set_target_properties(async PROPERTIES OUTPUT_NAME async)

install(TARGETS async
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin)
install(FILES include/async/async.h DESTINATION include/async)

add_executable(bulk source/main.cpp)
target_link_libraries(bulk PRIVATE async)
install(TARGETS bulk RUNTIME DESTINATION bin)

add_executable(bulk_server source/server.cpp)
target_link_libraries(bulk_server PRIVATE async Threads::Threads)
install(TARGETS bulk_server RUNTIME DESTINATION bin)

set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_NAME "async")
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Savchenko")

include(CPack)



