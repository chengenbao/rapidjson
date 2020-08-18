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

#include "tubemq/client_codec.h"

#include "tubemq/const_config.h"
#include "tubemq/const_rpc.h"
#include "tubemq/tubemq_errcode.h"
#include "tubemq/utils.h"


namespace tubemq {

using std::exception;



RequestWrapper::RequestWrapper() {
  this->method_id_ = rpc_config::kMethodInvalid;
}

RequestWrapper::RequestWrapper(int32_t method_id,
  google::protobuf::Message prot_msg) {
  this->method_id_ = method_id;
  this->prot_msg_ = prot_msg;
}

void RequestWrapper::setMessageInfo(int32_t method_id,
  google::protobuf::Message prot_msg) {
  this->method_id_ = method_id;
  this->prot_msg_ = prot_msg;
}


ResponseWrapper::ResponseWrapper() {
  success_ = false;
  code_ = err_code::kErrForbidden;
  serial_no_ = tb_config::kInvalidValue;
}

ResponseWrapper::ResponseWrapper(bool success,
  int32_t code, const string& error_msg) {
  success_ = success;
  code_ = code;
  error_msg_ = error_msg;
}


bool DecEncoder::Encode(const Any &in, BufferPtr &buff) {
  string msg_req;
  RequestWrapper req_wapper = (RequestWrapper)in;
  bool result = req_wapper.GetMessage().SerializeToString(msg_req);
  if (!result) {
    return result;
  }
  //
  string body_str;
  RequestBody req_body;
  req_body.set_method(req_wapper.GetMethodId());
  req_body.set_timeout(this->config_.GetRpcReadTimeoutMs());
  req_body.set_request(msg_req);
  result = req_body.SerializeToString(body_str);
  if (!result) {
    return result;
  }
  //
  string req_str;
  RequestHeader req_header;
  req_header.set_servicetype(Utils::GetServiceTypeByMethodId(req_wapper.GetMethodId()));
  req_header.set_protocolver(2);
  result = req_header.SerializeToString(req_str);
  if (!result) {
    return result;
  }
  //
  string rpc_str;
  RpcConnHeader rpc_header;
  rpc_header.set_flag(rpc_config::kRpcFlagMsgRequest);
  result = rpc_header.SerializeToString(rpc_str);
  if (!result) {
    return result;
  }
  // calc total list size 
  int32_t buff_cnt = 0;
  int32_t list_size =
    calcBlockCount(rpc_str.length())
    + calcBlockCount(req_str.length())
    + calcBlockCount(body_str.length());
  //
  buff = std::make_shared<Buffer>();
  buff->AppendInt32((int32_t) rpc_config::kRpcPrtBeginToken);
  buff->AppendInt32((int32_t) req_wapper->getRequestId());
  buff->AppendInt32(list_size);
  appendContent(buff, rpc_str);
  appendContent(buff, req_str);
  appendContent(buff, body_str);
  return true;
}

void DecEncoder::appendContent(BufferPtr &buff, string &content_str) {
  int32_t remain = 0;
  int32_t step_len = 0;
  int32_t buff_cnt = 0;
  buff_cnt = calcBlockCount(content_str.length());
  for (int32_t i = 0; i < buff_cnt; i++) {
    remain = content_str.length() - i * (Buffer::kInitialSize - 4);
    if (remain > Buffer::kInitialSize - 4) {
      step_len = Buffer::kInitialSize - 4;
    } else {
      step_len = content_str.length();
    }
    buff->AppendInt32(step_len);
    buff->Write(content_str.c_str() + i * (Buffer::kInitialSize - 4), step_len);
  }
}


int32_t DecEncoder::calcBlockCount(int32_t content_len) {
  int32_t block_cnt = content_len / Buffer::kInitialSize;
  int32_t remain_size = content_len % Buffer::kInitialSize;
  if (remain_size > 0) {
    block_cnt++;
  }
  if ((block_cnt * Buffer::kInitialSize) < (content_len + block_cnt * 4)) {
    block_cnt++;
  }
  return block_cnt;
}

bool DecEncoder::Decode(const BufferPtr &buff, Any &out) {
  // check package is valid
  RpcConnHeader   rpc_header;
  ResponseHeader  rsp_header;
  ResponseWrapper rsp_context;
  int total_len = buff->length();
  if (total_len <= 0) {
    // print log
    return false;
  }
  // parse pb data
  google::protobuf::io::ArrayInputStream rawOutput(buff->data(), total_len);
  bool result = readDelimitedFrom(&rawOutput, &rpc_header);
  if (!result) {
    return result;
  }
  result = readDelimitedFrom(&rawOutput, &rsp_header);
  if (!result) {
    return result;
  }
  ResponseHeader_Status rspStatus = rsp_header.status();
  if (rspStatus == ResponseHeader_Status_SUCCESS) {
    RspResponseBody response_body;
    rsp_context.SetSuccess(true);
    rsp_context.SetErrCode(err_code::kErrSuccess);
    rsp_context.SetErrMessage("OK");
    result = readDelimitedFrom(&rawOutput, &response_body);
    if (!result) {
      return false;
    }
    rsp_context.SetMethod(response_body.method());
    rsp_context.SetBusMessage(response_body);
  } else {
    RspExceptionBody rpc_exception;
    rsp_context.SetSuccess(false);
    result = readDelimitedFrom(&rawOutput, &rpc_exception);
    if (!result) {
      return false;
    }
    string errInfo = rpc_exception.exceptionname();
    errInfo += delimiter::kDelimiterPound;
    errInfo += rpc_exception.stacktrace();
    rsp_context.SetErrCode(err_code::kErrRcvThrowError);
    rsp_context.SetErrMessage(errInfo);
  }
  out = rsp_context;
  return false;
}

int32_t DecEncoder::Check(BufferPtr &in, Any &out, 
                    uint32_t &request_id, bool &has_request_id) {
  // check package is valid
  if (in->length() < 12) {
    return 0;
  }
  // check frameToken
  if (in->ReadUint32() != rpc_config::kRpcPrtBeginToken) {
    return -1;
  }
  // get request_id
  request_id = in->ReadUint32();
  has_request_id = true;
  // check list size
  uint32_t list_size = in->ReadUint32();
  if (list_size > rpc_config::kRpcMaxFrameListCnt) {
    return -1;
  }
  // check data list
  int32_t item_len = 0;
  int32_t read_len = 12;
  BufferPtr buf = std::make_shared<Buffer>();
  for (uint32_t i = 0; i < list_size; i++) {
    if (in->length() < 4) {
      return 0;
    }
    item_len = in->ReadInt32();
    if (item_len < 0) {
        return -1;
    }
    read_len += 4;
    if (item_len > in->length()) {
      return 0;
    }
    buf->Write(in->data(), item_len);
    read_len += item_len;
  }
  out = buf;
  return read_len;
}

bool DecEncoder::readDelimitedFrom(
  google::protobuf::io::ZeroCopyInputStream* rawInput,
  google::protobuf::MessageLite* message) {
  // We create a new coded stream for each message.  Don't worry, this is fast,
  // and it makes sure the 64MB total size limit is imposed per-message rather
  // than on the whole stream.  (See the CodedInputStream interface for more
  // info on this limit.)
  google::protobuf::io::CodedInputStream input(rawInput);

  // Read the size.
  uint32_t size;
  if (!input.ReadVarint32(&size)) return false;
  // Tell the stream not to read beyond that size.
  google::protobuf::io::CodedInputStream::Limit limit =
      input.PushLimit(size);
  // Parse the message.
  if (!message->MergeFromCodedStream(&input)) return false;
  if (!input.ConsumedEntireMessage()) return false;

  // Release the limit.
  input.PopLimit(limit);

  return true;
}

bool DecEncoder::writeDelimitedTo(
  const google::protobuf::MessageLite& message,
  google::protobuf::io::ZeroCopyOutputStream* rawOutput) {
  // We create a new coded stream for each message.  Don't worry, this is fast.
  google::protobuf::io::CodedOutputStream output(rawOutput);

  // Write the size.
  const int32_t size = message.ByteSize();
  output.WriteVarint32(size);

  uint8_t* buffer = output.GetDirectBufferForNBytesAndAdvance(size);
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


}  // namespace tubemq
