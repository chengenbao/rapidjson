CMAKE_MINIMUM_REQUIRED(VERSION 3.1)                                             

PROJECT(TubeMQThirdParty)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error -Wno-extra")
set(THIRD_PARTY_PATH ${CMAKE_BINARY_DIR}/third_party/)

include(FetchContent)
include(ExternalProject)

set(RAPIDJSON_INSTALL_DIR ${THIRD_PARTY_PATH}/rapidjson)
FetchContent_Populate(download_rapidjson
    URL "https://github.com/Tencent/rapidjson/archive/v1.1.0.tar.gz"
    SOURCE_DIR  ${RAPIDJSON_INSTALL_DIR}
)


ExternalProject_Add(
    rapidjson_proj
    PREFIX ${RAPIDJSON_INSTALL_DIR}
    SOURCE_DIR ${RAPIDJSON_INSTALL_DIR}
    CMAKE_ARGS -DRAPIDJSON_BUILD_TESTS=OFF -DRAPIDJSON_BUILD_DOC=OFF -DRAPIDJSON_BUILD_EXAMPLES=OFF
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
    )
INCLUDE_DIRECTORIES(${RAPIDJSON_INSTALL_DIR}/include)


set(SNAPPY_INSTALL_DIR ${THIRD_PARTY_PATH}/snappy)
FetchContent_Populate(download_snappy
    URL https://github.com/google/snappy/archive/1.1.8.tar.gz
    SOURCE_DIR  ${SNAPPY_INSTALL_DIR}
)

ExternalProject_Add(
    snappy_proj
    PREFIX ${SNAPPY_INSTALL_DIR}
    SOURCE_DIR ${SNAPPY_INSTALL_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND bash -c "cmake ./ && make -j32"
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
    )

include_directories(${SNAPPY_INSTALL_DIR})

add_library(snappy STATIC IMPORTED GLOBAL)
set_property(TARGET snappy PROPERTY
    IMPORTED_LOCATION ${SNAPPY_INSTALL_DIR}/libsnappy.a)
add_dependencies(snappy extern_snappy)


set(LOG4CPLUS_INSTALL_DIR ${THIRD_PARTY_PATH}/log4cplus)
FetchContent_Populate(download_log4cplus
    URL https://github.com/log4cplus/log4cplus/releases/download/REL_2_0_8/log4cplus-2.0.8.tar.gz
    SOURCE_DIR  ${LOG4CPLUS_INSTALL_DIR}
)

ExternalProject_Add(
    log4cplus_proj
    PREFIX ${LOG4CPLUS_INSTALL_DIR}
    SOURCE_DIR ${LOG4CPLUS_INSTALL_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND bash -c "cmake -DBUILD_SHARED_LIBS=OFF ./ && make -j32"
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
    )
include_directories(${LOG4CPLUS_INSTALL_DIR}/include)
add_library(log4cplus STATIC IMPORTED GLOBAL)
set_property(TARGET log4cplus PROPERTY
    IMPORTED_LOCATION  ${LOG4CPLUS_INSTALL_DIR}/src/liblog4cplusS.a)
add_dependencies(log4cplus log4cplus_proj)


set(ASIO_INSTALL_DIR ${THIRD_PARTY_PATH}/asio)
FetchContent_Populate(download_asio
    URL https://github.com/chriskohlhoff/asio/archive/asio-1-18-0.tar.gz 
    SOURCE_DIR  ${ASIO_INSTALL_DIR}
)

ExternalProject_Add(
    asio_proj
    PREFIX ${ASIO_INSTALL_DIR}
    SOURCE_DIR ${ASIO_INSTALL_DIR}
    CONFIGURE_COMMAND bash -c "cd asio && ./autogen.sh && ./configure CFLAGS=-std=c++11 CPPFLAGS=-std=c++11 CXXFLAGS=-std=c++11"
    BUILD_COMMAND bash -c "cd asio && make -j32"
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND ""
    )

include_directories(${ASIO_INSTALL_DIR}/asio/include)



