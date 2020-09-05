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

#include <assert.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>

#include <string>

#include "BrokerService.pb.h"
#include "MasterService.pb.h"
#include "RPC.pb.h"
#include "tubemq/any.h"
#include "tubemq/buffer.h"
#include "tubemq/codec_protocol.h"
#include "tubemq/const_config.h"
#include "tubemq/const_rpc.h"
#include "tubemq/tubemq_errcode.h"
#include "tubemq/tubemq_return.h"
#include "tubemq/utils.h"

namespace tubemq {

class TubeMQCodec final : public CodecProtocol {
 public:
  struct ReqProtocol {
    int32_t rpc_read_timeout_;
    uint32_t request_id_;
    int32_t method_id_;
    string prot_msg_;
  };

  struct RspProtocol {
    int32_t serial_no_;
    bool success_;
    int32_t code_;
    string error_msg_;
    int64_t message_id_;
    int32_t method_;
    RspResponseBody rsp_body_;
  };
  using ReqProtocolPtr = std::shared_ptr<ReqProtocol>;
  using RspProtocolPtr = std::shared_ptr<RspProtocol>;

 public:
  TubeMQCodec() {}

  virtual ~TubeMQCodec() {}

  virtual std::string Name() const { return "tubemq_v1"; }

  virtual bool Decode(const BufferPtr &buff, Any &out) {
    // check total length
    printf("\n come here, Decode data in \n");
    int32_t total_len = buff->length();
    if (total_len <= 0) {
      printf("\n total_len <= 0, out, total_len = %d \n", total_len);
      // print log
      return false;
    }
    // check package is valid
    RpcConnHeader rpc_header;
    ResponseHeader rsp_header;
    RspProtocolPtr rsp_protocol = GetRspProtocol();
    // parse pb data
    google::protobuf::io::ArrayInputStream rawOutput(buff->data(), total_len);
    bool result = readDelimitedFrom(&rawOutput, &rpc_header);
    if (!result) {
      printf("\n parse RpcConnHeader failure, out \n");
      return result;
    }
    result = readDelimitedFrom(&rawOutput, &rsp_header);
    if (!result) {
      printf("\n parse ResponseHeader failure, out \n");
      return result;
    }
    ResponseHeader_Status rspStatus = rsp_header.status();
    if (rspStatus == ResponseHeader_Status_SUCCESS) {
      RspResponseBody response_body;
      rsp_protocol->success_ = true;
      rsp_protocol->code_ = err_code::kErrSuccess;
      rsp_protocol->error_msg_ = "OK";
      result = readDelimitedFrom(&rawOutput, &response_body);
      if (!result) {
        printf("\n parse RspResponseBody failure, out \n");
        return false;
      }
      rsp_protocol->method_ = response_body.method();
      rsp_protocol->rsp_body_ = response_body;
    } else {
      RspExceptionBody rpc_exception;
      rsp_protocol->success_ = false;
      result = readDelimitedFrom(&rawOutput, &rpc_exception);
      if (!result) {
        printf("\n parse RspExceptionBody failure, out \n");
        return false;
      }
      string errInfo = rpc_exception.exceptionname();
      errInfo += delimiter::kDelimiterPound;
      errInfo += rpc_exception.stacktrace();
      rsp_protocol->code_ = err_code::kErrRcvThrowError;
      rsp_protocol->error_msg_ = errInfo;
    }
    out = Any(rsp_protocol);
    printf("\n parse Decode success , out \n");
    return true;
  }

  virtual bool Encode(const Any &in, BufferPtr &buff) {
    RequestBody req_body;
    ReqProtocolPtr req_protocol = any_cast<ReqProtocolPtr>(in);
    req_body.set_method(req_protocol->method_id_);
    req_body.set_timeout(req_protocol->rpc_read_timeout_);
    req_body.set_request(req_protocol->prot_msg_);
    RequestHeader req_header;
    req_header.set_servicetype(Utils::GetServiceTypeByMethodId(req_protocol->method_id_));
    req_header.set_protocolver(2);
    RpcConnHeader rpc_header;
    rpc_header.set_flag(rpc_config::kRpcFlagMsgRequest);
    // calc total list size
    uint32_t serial_len = 4 + rpc_header.ByteSizeLong()
                        + 4 + req_header.ByteSizeLong()
                        + 4 + req_body.ByteSizeLong();
    uint8_t *step_buff = (uint8_t *)malloc(serial_len);
    assert(step_buff);
    google::protobuf::io::ArrayOutputStream rawOutput(step_buff, serial_len);
    bool result = writeDelimitedTo(rpc_header, &rawOutput);
    if(!result) {
      delete[] step_buff;
      return result;
    }
    result = writeDelimitedTo(req_header, &rawOutput);
    if(!result) {
      delete[] step_buff;
      return result;
    }
    result = writeDelimitedTo(req_body, &rawOutput);
    if(!result) {
      delete[] step_buff;
      return result;
    }
    // append data to buffer
    uint32_t list_size = calcBlockCount(serial_len);
    buff->AppendInt32((int32_t)rpc_config::kRpcPrtBeginToken);
    buff->AppendInt32((int32_t)req_protocol->request_id_);
    buff->AppendInt32(list_size);
    uint32_t write_pos = 0;
    for (uint32_t i = 0; i < list_size; i++) {
      uint32_t slice_len = serial_len - i * Buffer::kInitialSize;
      if (slice_len > Buffer::kInitialSize) {
        slice_len = Buffer::kInitialSize;
      }
      printf("\n slice_len = %d, serial_len = %d\n", slice_len, serial_len);

      buff->AppendInt32(slice_len);
      buff->Write(step_buff + write_pos, slice_len);
      write_pos += slice_len;
    }
    delete[] step_buff;
    return true;
  }

  // return code: -1 failed; 0-Unfinished; > 0 package buffer size
  virtual int32_t Check(BufferPtr &in, Any &out, uint32_t &request_id, bool &has_request_id) {
    printf("\n come here, check data in \n");
    uint32_t readed_len = 0;
    // check package is valid
    if (in->length() < 12) {
      printf("\n come here, in->length < 12, is %ld \n", in->length());
      return 0;
    }
    // check frameToken
    if (in->ReadUint32() != rpc_config::kRpcPrtBeginToken) {
      printf("\n come here, first token != rpc_config::kRpcPrtBeginToken, is %d \n", in->ReadUint32());
      return -1;
    }
    readed_len += 4;
    // get request_id
    request_id = in->ReadUint32();
    readed_len += 4;    
    has_request_id = true;
    // check list size
    uint32_t list_size = in->ReadUint32();
    if (list_size > rpc_config::kRpcMaxFrameListCnt) {
      printf("\n come here, list_size over max, is %d \n", list_size);
      return -1;
    }
    readed_len += 4;    
    // check data list
    uint32_t item_len = 0;
    auto buf = std::make_shared<Buffer>();
    for (uint32_t i = 0; i < list_size; i++) {
      if (in->length() < 4) {
        printf("\n come here, buffer Remaining length < 4, is %ld \n", in->length());
        return 0;
      }
      item_len = in->ReadUint32();
      readed_len += 4;
      if (item_len < 0) {
        printf("\n come here, slice length < 0, is %d \n", in->ReadUint32());
        return -1;
      }
      if (item_len > in->length()) {
        printf("\n come here, item_len > Remaining length, item_len is %d, in->length() = %ld \n", item_len, in->length());
        return 0;
      }
      buf->Write(in->data(), item_len);
      readed_len += item_len;
    }
    out = buf;
    return item_len;
  }

  static ReqProtocolPtr GetReqProtocol() { return std::make_shared<ReqProtocol>(); }
  static RspProtocolPtr GetRspProtocol() { return std::make_shared<RspProtocol>(); }

  static bool readDelimitedFrom(google::protobuf::io::ZeroCopyInputStream *rawInput,
                                google::protobuf::MessageLite *message) {
    // We create a new coded stream for each message.  Don't worry, this is fast,
    // and it makes sure the 64MB total size limit is imposed per-message rather
    // than on the whole stream.  (See the CodedInputStream interface for more
    // info on this limit.)
    google::protobuf::io::CodedInputStream input(rawInput);

    // Read the size.
    uint32_t size;
    if (!input.ReadVarint32(&size)) return false;
    // Tell the stream not to read beyond that size.
    google::protobuf::io::CodedInputStream::Limit limit = input.PushLimit(size);
    // Parse the message.
    if (!message->MergeFromCodedStream(&input)) return false;
    if (!input.ConsumedEntireMessage()) return false;

    // Release the limit.
    input.PopLimit(limit);

    return true;
  }

  static bool writeDelimitedTo(const google::protobuf::MessageLite &message,
                               google::protobuf::io::ZeroCopyOutputStream *rawOutput) {
    // We create a new coded stream for each message.  Don't worry, this is fast.
    google::protobuf::io::CodedOutputStream output(rawOutput);

    // Write the size.
    const int32_t size = message.ByteSizeLong();
    output.WriteVarint32(size);

    uint8_t *buffer = output.GetDirectBufferForNBytesAndAdvance(size);
    if (buffer != NULL) {
      // Optimization:  The message fits in one buffer, so use the faster
      // direct-to-array serialization path.
      message.SerializeWithCachedSizesToArray(buffer);
    } else {
      // Slightly-slower path when the message is multiple buffers.
      message.SerializeWithCachedSizes(&output);
      if (output.HadError()) return false;
    }

    return true;
  }

  uint32_t calcBlockCount(uint32_t content_len) {
    uint32_t block_cnt = content_len / Buffer::kInitialSize;
    uint32_t remain_size = content_len % Buffer::kInitialSize;
    if (remain_size > 0) {
      block_cnt++;
    }
    return block_cnt;
  }
};
}  // namespace tubemq
#endif

