<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->


# TubeMQ C++ client library
## Requirements

 * CMake
 * [ASIO](https://github.com/chriskohlhoff/asio.git)
 * [OpenSSL](https://github.com/openssl/openssl.git)
 * [Protocol Buffer](https://developers.google.com/protocol-buffers/) ./configure --disable-shared CFLAGS="-fPIC" CXXFLAGS="-fPIC" && make && make install
 * [Log4cplus](https://github.com/log4cplus/log4cplus.git)
 * [Rapidjson](https://github.com/Tencent/rapidjson.git) 

# Step to build
  * install protobuf (./configure --disable-shared CFLAGS="-fPIC" CXXFLAGS="-fPIC" && make && make install)
  * ./build_linux.sh
  * cd release/
  * chmod +x release_linux.sh
  * ./release_linux.sh 
 

# 版本修改历史

## 0.1.6-C-0.5.0 
- 1. 服务启动由配置文件改为配置对象，避免业务要做额外设置

## 0.1.3-C-0.5.0 
- 1. 对依赖包snappy增加-fPIC的编译选项，避免第三方引用报错

## 0.1.2-C-0.5.0 
- 1. 给DataItem结构里的data结尾补0，解决有些业务将data当作以0结尾的数据处理的问题
- 2. 将部分const变量改为enum，解决编译告警问题
 
## 0.1.1-0.5.0
 - 1. 基于0.5.0版本的TubeMQ协议进行重新构建TubeMQ的SDK，后续版本都基于此进行
 
