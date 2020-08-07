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


namespace tubemq {

using std::exception;



ResponseContext::ResponseContext() {
  success_ = false;
  code_ = err_code::kErrForbidden;
  serial_no_ = tb_config::kInvalidValue; 
}

ResponseContext::ResponseContext(bool success, 
  int32_t code, const string& error_msg) {
  success_ = success;
  code_ = code;
  error_msg_ = error_msg; 
}



bool DecEncoder::Encode(const Any &in, BufferPtr &buff) {
  return false;
}

bool DecEncoder::Decode(const BufferPtr &buff, Any &out) {
  return false;
}

int32_t DecEncoder::Check(BufferPtr &in, Any &out,
                    int32_t &request_id, bool &has_request_id) {
  int32_t i = 0;
  // check package is valid
  if (in->length() < 12) {
    return 0;
  }
  // check frameToken
  if ((uint32_t)in->ReadInt32() != rpc_config::kRpcPrtBeginToken) {
    return -1;
  }
  // get request_id
  request_id = in->ReadInt32();
  has_request_id = true;
  // check list size
  int32_t list_size = in->ReadInt32();
  if (list_size <= 0 
    || (uint32_t)list_size > rpc_config::kRpcMaxFrameListCnt) {
    return -1;
  }
  // check data list
  int32_t ttl_len = 0;
  int32_t item_len = 0;
  int32_t read_len = 12;
  for (i = 0; i < list_size; i++) {
    if (in->length() < 4) {
      return 0;
    }
    item_len = in->ReadInt32();
    read_len += 4;
    if ((uint32_t)item_len > in->length()) {
      return 0;
    }
    in->Skip(item_len);
    ttl_len += item_len;
    read_len += item_len;
  }
  // copy data
  char* msg_content = NULL;
  msg_content = (char *)calloc(ttl_len, 1);
  if (msg_content == NULL) {
    // need return other value ?
    return -1;
  }
  int32_t cpy_pos = 0;
  int32_t read_pos = 12;
  char* orig_begin = in->begin();
  orig_begin += read_pos;
  for (i = 0; i < list_size; i++) {
    item_len = ntohl(*(int *)(&orig_begin[read_pos]));
    memcpy(msg_content + cpy_pos, orig_begin + read_pos, item_len);
    cpy_pos += item_len;
    read_pos += 4 + item_len;
  }
  // parse pb data
  bool result = parseProtobufRsp(request_id, msg_content, ttl_len, out);
  free(msg_content);
  if (!result) {
    // need return other value ?
    return -1;
  }
  return read_len;
}


bool DecEncoder::parseProtobufRsp(int32_t serial_no, 
                             char* message, int32_t msgLen, Any &out) {
  RpcConnHeader   rpc_header;
  ResponseHeader  rsp_header;
  ResponseContext rsp_context;
  try {
    google::protobuf::io::ArrayInputStream rawOutput(message, msgLen);
    bool result = readDelimitedFrom(&rawOutput, &rpc_header);
    if (!result) {
      return result;
    }
    result = readDelimitedFrom(&rawOutput, &rsp_header);
    if (!result) {
      return result;
    }
    rsp_context.SetSerialNo(serial_no);
    ResponseHeader_Status rspStatus = rsp_header.status();
    if (rspStatus == ResponseHeader_Status_SUCCESS){
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
  } catch (exception &err) {
    return false;
  }
  return true;  
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
