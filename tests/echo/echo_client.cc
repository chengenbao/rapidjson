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
#include "echo_codec.h"
#include "executor_pool.h"
#include "logger.h"
#include "singleton.h"

using namespace tubemq;

template <typename RequestProtocol>
Future<ResponseContext> asyncEchoRequest(ConnectionPoolPtr pool, RequestContextPtr& request,
                                         RequestProtocol& protocol) {
  request->buf_ = std::make_shared<Buffer>();
  Any in(protocol);
  request->codec_->Encode(in, request->buf_);
  auto future = request->promise_.GetFuture();
  pool->GetConnection(request)->AsyncWrite(request);
  return future;
}

void echoClient(ConnectionPoolPtr pool, const string& ip, uint16_t port) {
  while (1) {
    auto request = std::make_shared<RequestContext>();
    request->ip_ = ip;
    request->port_ = port;
    EchoCodec::ReqProtocolPtr req_protocol = EchoCodec::GetReqProtocol();
    req_protocol->data_ = "hello world";
    // build getmessage request
    request->codec_ = std::make_shared<EchoCodec>();
    request->timeout_ = 1000;
    request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
    req_protocol->request_id_ = request->request_id_;
    asyncEchoRequest(pool, request, req_protocol)
        .AddCallBack([](ErrorCode error, const ResponseContext& response_context) {
          if (error.Value() != err_code::kErrSuccess) {
            LOG_ERROR("recv error: %s", error.Message().c_str());
            return;
          }  // process response
          auto rsp = any_cast<EchoCodec::RspProtocolPtr>(response_context.rsp_);
          LOG_TRACE("rsp[%d]:%s", rsp->request_id_, rsp->data_.c_str());
        });
    std::this_thread::sleep_for(std::chrono::microseconds(500));
  }
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: client <host> <port>\n";
    return 1;
  }
  GetLogger().Init("tubemq", tubemq::Logger::Level::kTrace);
  auto pool = std::make_shared<ExecutorPool>(4);
  auto connection_pool = std::make_shared<ConnectionPool>(pool);

  std::string ip = argv[1];
  uint16_t port = std::atoi(argv[2]);
  std::vector<std::thread> workers;
  for (int i = 0; i < 2; ++i) {
    workers.emplace_back([&] { echoClient(connection_pool, ip, port); });
  }
  workers[0].join();
  return 0;
}

