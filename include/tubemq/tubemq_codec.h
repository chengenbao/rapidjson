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

#ifndef _TUBEMQ_TUBEMQ_CODEC_H_
#define _TUBEMQ_TUBEMQ_CODEC_H_

#include "tubemq/codec_protocol.h"

namespace tubemq {

template <class ReqBody, class RspBody, class Head>
class TubeMQCodec final : public CodecProtocol {
 public:
  struct ReqProtocol {
    Head head_;
    ReqBody req_body_;
  };

  struct RspProtocol {
    Head head_;
    RspBody rsp_body_;
  };
  using ReqProtocolPtr = std::shared_ptr<ReqProtocol>;
  using RspProtocolPtr = std::shared_ptr<RspProtocol>;

 public:
  TubeMQCodec() {}

  virtual ~TubeMQCodec() {}

  virtual std::string Name() const { return "tubemq_v1"; }

  virtual bool Decode(const BufferPtr &buff, Any &out) {
    RspProtocolPtr rsp = GetRspProtocol();
    out = rsp;
    // TODO
    return true;
  }

  virtual bool Encode(const Any &in, BufferPtr &buff) {
    // TODO
    ReqProtocolPtr req = any_cast<ReqProtocolPtr>(in);
    return true;
  }

  // return code: -1 failed; 0-Unfinished; > 0 package buffer size
  virtual int32_t Check(BufferPtr &in, Any &out, uint32_t &request_id, bool &has_request_id) {
    // TODO
    return in->length();
  }

  static ReqProtocolPtr GetReqProtocol() { return std::make_shared<ReqProtocol>(); }
  static RspProtocolPtr GetRspProtocol() { return std::make_shared<RspProtocol>(); }
};
}  // namespace tubemq
#endif

