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
    RspProtocolPtr rsp_protocol = GetRspProtocol();
    rsp_protocol->data_ = buff->ToString();
    out = Any(rsp_protocol);
    return true;
  }

  virtual bool Encode(const Any &in, BufferPtr &buff) {
    ReqProtocolPtr req_protocol = any_cast<ReqProtocolPtr>(in);
    buff->AppendInt32((int32_t)rpc_config::kRpcPrtBeginToken);
    buff->AppendInt32((int32_t)req_protocol->request_id_);
    buff->AppendInt32((int32_t)req_protocol->data_.size());
    buff->Write(req_protocol->data_.data(), req_protocol->data_.size());
    LOG_TRACE("Encode: encode message success, finished! request_id:%d", req_protocol->request_id_);
    return true;
  }

  // return code: -1 failed; 0-Unfinished; > 0 package buffer size
  virtual int32_t Check(BufferPtr &in, Any &out, uint32_t &request_id, bool &has_request_id,
                        size_t &package_length) {
    LOG_TRACE("check in:%s", in->String().c_str());
    // check package is valid
    if (in->length() < 12) {
      package_length = 12;
      LOG_TRACE("Check: data's length < 12, is %ld, out", in->length());
      return 0;
    }
    // check frameToken
    uint32_t token = in->ReadUint32();
    if (token != rpc_config::kRpcPrtBeginToken) {
      LOG_TRACE("Check: first token is illegal, is %d, out", token);
      return -1;
    }
    // get request_id
    request_id = in->ReadUint32();
    uint32_t length = in->ReadUint32();
    if (length > 10 * 1024 * 1024) {
      LOG_TRACE("Check: length over max, is %d, out", length);
      return -1;
    }
    size_t size = 12 + length;
    package_length = size;
    if (in->length() < length) {
      return 0;
    }
    auto output_buf = in->Slice();
    output_buf->Truncate(length);
    out = output_buf;
    return size;
  }

  static ReqProtocolPtr GetReqProtocol() { return std::make_shared<ReqProtocol>(); }
  static RspProtocolPtr GetRspProtocol() { return std::make_shared<RspProtocol>(); }
};
}  // namespace tubemq

#endif

