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

#ifndef TUBEMQ_CLIENT_API_H_
#define TUBEMQ_CLIENT_API_H_

#include <stdlib.h>

#include <list>
#include <string>

#include "tubemq/MasterService.pb.h"
#include "tubemq/atomic_def.h"
#include "tubemq/client_service.h"
#include "tubemq/client_subinfo.h"
#include "tubemq/rmt_data_cache.h"
#include "tubemq/tubemq_config.h"
#include "tubemq/tubemq_message.h"
#include "tubemq/tubemq_return.h"



namespace tubemq {

using std::string;



bool StartTubeMQService(string& err_info,
              const string& conf_file = "../conf/client.conf");
bool StopTubeMQService(string& err_info);




class TubeMQConsumer : public BaseClient {
 public:
  TubeMQConsumer();
  ~TubeMQConsumer();
  bool Start(string& err_info, const ConsumerConfig& config);
  void ShutDown();
  bool GetMessage(ConsumerResult& result);
  bool Confirm(const string& confirm_context,
         bool is_consumed, ConsumerResult& result);

 private:
  string buildUUID();
  bool register2Master(string& err_info, bool need_change);

 private:
  bool buidRegisterRequestC2M(string& err_info,
                           char** out_msg, int& out_length);
  bool buidHeartRequestC2M(string& err_info,
                           char** out_msg, int& out_length);
  bool buidCloseRequestC2M(string& err_info,
                           char** out_msg, int& out_length);
  bool processRegisterResponseM2C(
                    const RegisterResponseM2C& response);
  bool getSerializedMsg(string& err_info,
    char** out_msg, int& out_length, const string& req_msg,
    const int32_t method_id, int32_t serial_no);

 private:
  string client_uuid_;
  AtomicInteger status_;
  ConsumerConfig config_;
  ClientSubInfo sub_info_;
  RmtDataCacheCsm rmtdata_cache_;
  int32_t cur_report_times_;
};


}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_API_H_
