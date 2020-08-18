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

#include "tubemq/codec_protocol.h"

using namespace tubemq;

template <class ReqBody, class RspBody, class Head>
bool TubeMQCodec<Head, Body>::Decode(const BufferPtr &buff, Any &out) {
  return true;
}

template <class ReqBody, class RspBody, class Head>
bool TubeMQCodec<Head, Body>::Encode(const Any &in, BufferPtr &buff) {
  return true;
}

// return code: -1 failed; 0-Unfinished; > 0 package buffer size
template <class ReqBody, class RspBody, class Head>
int32_t TubeMQCodec<Head, Body>::Check(BufferPtr &in, Any &out, uint32_t &request_id,
                                       bool &has_request_id) {
  return in.length();
}
