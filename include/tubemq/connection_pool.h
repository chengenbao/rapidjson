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

#ifndef _TUBEMQ_CONNECTION_POLL_
#define _TUBEMQ_CONNECTION_POLL_

#include <stdlib.h>

#include <chrono>
#include <ctime>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>

#include "asio.hpp"
#include "tubemq/client_connection.h"
#include "tubemq/connection.h"
#include "tubemq/noncopyable.h"
#include "tubemq/transport.h"

namespace tubemq {
class ConnectionPool : noncopyable {
 public:
  explicit ConnectionPool(ExecutorPoolPtr& executor_pool)
      : executor_pool_(executor_pool), regular_timer_(executor_pool_->Get()->CreateSteadyTimer()) {}
  ~ConnectionPool() { Clear(); }

  void Clear() {
    Lock lock(mutex_);
    for (auto& connection : connection_pool_) {
      connection.second->Close();
    }
    connection_pool_.clear();
  }

  ConnectionPtr GetConnection(RequestContextPtr& request) {
    std::string key = generateConnectionKey(request);

    Lock lock(mutex_);
    auto it = connection_pool_.find(key);
    if (it != connection_pool_.end() && !(it->second->IsStop())) {
      return it->second;
    }
    auto executor = executor_pool_->Get();
    auto connect = std::make_shared<ClientConnection>(executor, request->ip_, request->port_);
    connection_pool_[key] = connect;
    connect->SetCloseNotifier(request->close_notifier_);

    auto codec = request->codec_;
    connect->SetProtocalCheck(
        [codec](BufferPtr& in, Any& out, int32_t& request_id, bool& has_request_id) -> int {
          return codec->Check(in, out, request_id, has_request_id);
        });
    return connect;
  }

 private:
  inline std::string generateConnectionKey(const RequestContextPtr& request) {
    std::string key;
    key += request->ip_;
    key += "_";
    key += std::to_string(request->port_);
    key += "_";
    key += std::to_string(request->connection_pool_id_);
    return key;
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, ConnectionPtr> connection_pool_;
  ExecutorPoolPtr executor_pool_;
  SteadyTimerPtr regular_timer_;
  typedef std::unique_lock<std::mutex> Lock;
};

using ConnectionPoolPtr = std::shared_ptr<ConnectionPool>;
}  // namespace tubemq

#endif  // _TUBEMQ_CONNECTION_POLL_

