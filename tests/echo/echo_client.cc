/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <chrono>
#include <exception>
#include <functional>
#include <iostream>
#include <string>
#include <thread>

#include "connection_pool.h"
#include "executor_pool.h"
#include "logger.h"

using namespace tubemq;

int main(int argc, char* argv[]) {
  using namespace std::placeholders;  // for _1, _2, _3...
  if (argc != 3) {
    std::cerr << "Usage: client <host> <port>\n";
    return 1;
  }
  GetLogger().Init("tubemq", tubemq::Logger::Level::kDebug);
  auto pool = std::make_shared<ExecutorPool>(4);
  auto request = std::make_shared<RequestContext>();
  request->ip_ = argv[1];
  request->port_ = std::atoi(argv[2]);
  auto connection_pool = std::make_shared<ConnectionPool>(pool);
  auto connect = connection_pool->GetConnection(request);
  // connect->AsyncWrite(request);
  std::this_thread::sleep_for(std::chrono::seconds(180));
  return 0;
}

