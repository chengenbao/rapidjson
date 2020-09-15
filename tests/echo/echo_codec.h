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

#ifndef _TUBEMQ_TESTS_ECHO_H_
#define _TUBEMQ_TESTS_ECHO_H_
#include <string>

#include "codec_protocol.h"
#include "const_config.h"
#include "const_rpc.h"
#include "logger.h"
#include "tubemq/tubemq_errcode.h"
#include "tubemq/tubemq_return.h"
#include "utils.h"

namespace tubemq {
// format token ruquestid len string
class EchoCodec final : public CodecProtocol {
 public:
  struct ReqProtocol {
    uint32_t request_id_;
    string data_;
  };

  struct RspProtocol {
    uint32_t request_id_;
    string data_;
  };
  using ReqProtocolPtr = std::shared_ptr<ReqProtocol>;
  using RspProtocolPtr = std::shared_ptr<RspProtocol>;

 public:
  EchoCodec() {}

  virtual ~EchoCodec() {}

  virtual std::string Name() const { return "echo"; }

  virtual bool Decode(const BufferPtr &buff, Any &out) {
    // check total length
    LOG_TRACE("Decode: full message Decode come, begin decode");
    out = Any(rsp_protocol);
    LOG_TRACE("Decode: decode message success, finished");
    return true;
  }

  virtual bool Encode(const Any &in, BufferPtr &buff) {
    RequestBody req_body;
    ReqProtocolPtr req_protocol = any_cast<ReqProtocolPtr>(in);

    LOG_TRACE("Encode: encode message success, finished!");
    return true;
  }

  // return code: -1 failed; 0-Unfinished; > 0 package buffer size
  virtual int32_t Check(BufferPtr &in, Any &out, uint32_t &request_id, bool &has_request_id,
                        size_t &package_length) {
    LOG_TRACE("check in:%s", in->String().c_str());
    return readed_len;
  }

  static ReqProtocolPtr GetReqProtocol() { return std::make_shared<ReqProtocol>(); }
  static RspProtocolPtr GetRspProtocol() { return std::make_shared<RspProtocol>(); }
};
}  // namespace tubemq

#endif

