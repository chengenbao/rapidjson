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

#ifndef TUBEMQ_CLIENT_CODEC_H_
#define TUBEMQ_CLIENT_CODEC_H_

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>

#include <string>

#include "tubemq/any.h"
#include "tubemq/buffer.h"
#include "tubemq/codec_protocol.h"
#include "tubemq/tubemq_return.h"
#include "tubemq/MasterService.pb.h"
#include "tubemq/RPC.pb.h"
#include "tubemq/BrokerService.pb.h"



namespace tubemq {

using std::string;


class RequestWrapper {
 public:
  RequestWrapper();
  RequestWrapper(int32_t method_id, google::protobuf::Message prot_msg);
  void setRequestId(uint32_t request_id) { this.request_id_ = request_id; }
  uint32_t getRequestId() const { return this.request_id_; }
  void setMessageInfo(int32_t method_id, google::protobuf::Message prot_msg);
  int32_t GetMethodId() const { return method_id_; }
  const google::protobuf::Message& GetMessage() const { return prot_msg_; }
  
 private:
  uint32_t request_id_;
  int32_t method_id_;
  google::protobuf::Message prot_msg_;
};


class ResponseWrapper {
 public:
  ResponseWrapper();
  ResponseWrapper(bool success, int32_t code, const string& error_msg);
  int32_t GetSerialNo() const { return serial_no_; }
  void SetSerialNo(int32_t serial) { serial_no_ = serial; }
  bool IsSuccess() const { return success_; }
  void SetSuccess(bool success) { success_ = success; }
  int32_t  GetErrCode() const { return code_; }
  void SetErrCode(int32_t code) { code_ = code; }
  const string& GetErrMessage() const { return error_msg_; }
  void SetErrMessage(const string& error_msg) { error_msg_ = error_msg; }
  int64_t GetMessageId() const { return message_id_; }
  void SetMessageId(int64_t message_id) { message_id_ = message_id; }
  int32_t GetMethod() const { return method_; }
  void SetMethod(int32_t method) { method_ = method; }
  void SetBusMessage(const RspResponseBody& response_msg) { rpc_response_ = response_msg; }
  const RspResponseBody& GetBusMessage() const { return rpc_response_; }

 private:
  int32_t  serial_no_;
  bool success_;
  int32_t  code_;
  string error_msg_;
  int64_t message_id_;
  int32_t method_;
  RspResponseBody rpc_response_;
};


class DecEncoder : public CodecProtocol {
 public:
  DecEncoder();
  ~DecEncoder();
  string Name() const;
  bool Decode(const BufferPtr &buff, Any &out);
  bool Encode(const Any &in, BufferPtr &buff);
  // return code: -1 failed; 0-Unfinished; > 0 package buffer size
  int32_t Check(BufferPtr &in, Any &out, int32_t &request_id, bool &has_request_id);
  // get protocol request id
  int32_t GetRequestId(uint32_t &request_id, const Any &rsp) const;
  // set protocol request request id
  int32_t SetRequestId(uint32_t request_id, Any &req);

 private:
  bool parseProtobufRsp(uint32_t serial_no, char* message, int32_t msgLen, Any &out);
  bool readDelimitedFrom(
    google::protobuf::io::ZeroCopyInputStream* rawInput,
    google::protobuf::MessageLite* message);
  bool writeDelimitedTo(
    const google::protobuf::MessageLite& message,
    google::protobuf::io::ZeroCopyOutputStream* rawOutput);
};

}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_CODEC_H_
