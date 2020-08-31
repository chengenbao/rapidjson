CMAKE_MINIMUM_REQUIRED(VERSION 3.1)                                             

PROJECT(TubeMQThirdParty)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error")

include(ExternalProject)

ExternalProject_Add(
    log4cplus_proj
    URL https://github.com/log4cplus/log4cplus/releases/download/REL_2_0_5/log4cplus-2.0.5.tar.gz
    CONFIGURE_COMMAND ./configure --prefix=<INSTALL_DIR> --disable-shared --with-pic CFLAGS=-O2\  CXXFLAGS=-O2\ -fPIC
    TEST_BEFORE_INSTALL 0
    BUILD_IN_SOURCE 1
    INSTALL_DIR ${CMAKE_BINARY_DIR}/third_party/
    )

