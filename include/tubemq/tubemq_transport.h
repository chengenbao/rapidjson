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

#ifndef _TUBEMQ_TUBEMQ_TRANSPORT_H_
#define _TUBEMQ_TUBEMQ_TRANSPORT_H_

#include "tubemq/codec_protocol.h"
#include "tubemq/connection_pool.h"
#include "tubemq/executor_pool.h"
#include "tubemq/future.h"
#include "tubemq/logger.h"
#include "tubemq/transport.h"
#include "tubemq/tubemq_codec.h"

namespace tubemq {

template <class ReqBody, class RspBody, class Head>
Future<ResponseContext> AsyncRequest(RequestContextPtr& request,
                                     TubeMQCodec<ReqBody, RspBody, Head>::ReqProtocolPtr protocol);

}  // namespace tubemq

#endif  // _TUBEMQ_TUBEMQ_TRANSPORT_H_
