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
 * [Protocol Buffer](https://developers.google.com/protocol-buffers/) ./configure --disable-shared && make && make install
 * [Log4cplus](https://github.com/log4cplus/log4cplus.git)
 * [Rapidjson](https://github.com/Tencent/rapidjson.git) 

# Step to build
  * install protobuf
  * ./build_linux.sh
  * cd release/
  * chmod +x release_linux.sh
  * ./release_linux.sh 
 

# 版本修改历史
 
## 0.1.1-0.5.0
 - 1. 基于0.5.0版本的TubeMQ协议进行重新构建TubeMQ的SDK，后续版本都基于此进行
 
