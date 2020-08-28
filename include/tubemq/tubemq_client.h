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
#include <mutex>
#include <string>

#include "BrokerService.pb.h"
#include "RPC.pb.h"
#include "MasterService.pb.h"
#include "tubemq/atomic_def.h"
#include "tubemq/client_service.h"
#include "tubemq/client_subinfo.h"
#include "tubemq/meta_info.h"
#include "tubemq/rmt_data_cache.h"
#include "tubemq/tubemq_codec.h"
#include "tubemq/tubemq_config.h"
#include "tubemq/tubemq_message.h"
#include "tubemq/tubemq_return.h"



namespace tubemq {

using std::mutex;
using std::string;



bool StartTubeMQService(string& err_info,
              const string& conf_file = "../conf/client.conf");
bool StopTubeMQService(string& err_info);



class TubeMQConsumer : public BaseClient {
 public:
  TubeMQConsumer();
  ~TubeMQConsumer();
  bool Start(string& err_info, const ConsumerConfig& config);
  virtual void ShutDown();
  bool GetMessage(ConsumerResult& result);
  bool Confirm(const string& confirm_context,
         bool is_consumed, ConsumerResult& result);

 private:
  string buildUUID();
  bool isClientRunning();
  int32_t getConsumeReadStatus(bool is_first_reg);
  bool initMasterAddress(string& err_info, const string& master_info);
  void getNextMasterAddr(string& ipaddr, int32_t& port);
  void getCurrentMasterAddr(string& ipaddr, int32_t& port);
  bool register2Master(int32_t& error_code,
    string& err_info, bool need_change);
  bool heartBeat2Master(string& err_info);
  void heartBeat2Master();
  void close2Master();
  bool needGenMasterCertificateInfo(bool force);
  void genBrokerAuthenticInfo(AuthorizedInfo* p_authInfo, bool force);
  void processAuthorizedToken(const MasterAuthorizedInfo& authorized_token_info);
  

 private:
  void buidRegisterRequestC2M(TubeMQCodec::ReqProtocolPtr& req_protocol);
  void buidHeartRequestC2M(TubeMQCodec::ReqProtocolPtr& req_protocol);
  void buidCloseRequestC2M(TubeMQCodec::ReqProtocolPtr& req_protocol);
  void buidRegisterRequestC2B(const PartitionExt& partition,
    TubeMQCodec::ReqProtocolPtr& req_protocol);
  void buidUnRegRequestC2B(const PartitionExt& partition,
    bool is_last_consumed, TubeMQCodec::ReqProtocolPtr& req_protocol);
  void buidHeartBeatC2B(const list<PartitionExt>& partitions,
    TubeMQCodec::ReqProtocolPtr& req_protocol);
  void buidGetMessageC2B(const PartitionExt& partition,
    bool is_last_consumed, TubeMQCodec::ReqProtocolPtr& req_protocol);
  void buidCommitC2B(const PartitionExt& partition,
    bool is_last_consumed, TubeMQCodec::ReqProtocolPtr& req_protocol);
  void genMasterAuthenticateToken(AuthenticateInfo* pauthinfo,
    const string& username, const string usrpassword);
  bool processRegisterResponseM2C(int32_t& err_code,
    string& err_info, const TubeMQCodec::RspProtocolPtr& rsp_protocol);
  bool processHBResponseM2C(int32_t& error_code, string& err_info,
    const TubeMQCodec::RspProtocolPtr& rsp_protocol);

 private:
  int32_t client_indexid_;
  string client_uuid_;
  AtomicInteger status_;
  ConsumerConfig config_;
  ClientSubInfo sub_info_;
  RmtDataCacheCsm rmtdata_cache_;
  AtomicLong visit_token_;
  mutable mutex auth_lock_;
  string authorized_info_;
  AtomicBoolean nextauth_2_master;
  AtomicBoolean nextauth_2_broker;
  string curr_master_addr_;
  map<string, int32_t> masters_map_;
  bool is_master_actived_;
  int32_t master_sh_retry_cnt_;
  int64_t last_master_hbtime_;
  int32_t unreport_times_;
};


}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_API_H_
