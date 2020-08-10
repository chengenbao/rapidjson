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

#include "tubemq/tubemq_client.h"

#include <signal.h>
#include <unistd.h>
#include <sstream>
#include "tubemq/client_service.h"
#include "tubemq/const_config.h"
#include "tubemq/const_rpc.h"
#include "tubemq/logger.h"
#include "tubemq/tubemq_config.h"
#include "tubemq/utils.h"
#include "tubemq/version.h"




namespace tubemq {

using std::stringstream;


bool StartTubeMQService(string& err_info, string& conf_file) {
  signal(SIGPIPE, SIG_IGN);
  return TubeMQService::Instance()->Start(err_info, conf_file);
}

bool StopTubeMQService(string& err_info) {
  int32_t count = TubeMQService::Instance()->GetClientObjCnt();
  if (count > 0) {
    stringstream ss;
    ss << "Check found ";
    ss << count;
    ss << " clients not shutdown, please shutdown clients first!";
    err_info = ss.str();
    return false;
  }
  return TubeMQService::Instance()->Stop(err_info);
}

TubeMQConsumer::TubeMQConsumer() : BaseClient(false) {
  status_.Set(0);
  cur_report_times_ = 0;
  client_uuid_ = "";
  visit_token_.Set(tb_config::kInvalidValue);
  nextauth_2_master.Set(false);
  nextauth_2_broker.Set(false);
}

TubeMQConsumer::~TubeMQConsumer() {
  //
}

bool TubeMQConsumer::Start(string& err_info, const ConsumerConfig& config) {
  ConsumerConfig tmp_config;
  if (!this->status_.CompareAndSet(0, 1)) {
    err_info = "Ok";
    return true;
  }
  // check configure
  if (config.GetGroupName().length() == 0
    || config.GetMasterAddrInfo().length() == 0) {
    err_info = "Parameter error: not set master address info or group name!";
    return false;
  }
  //
  if (!TubeMQService::Instance()->IsRunning()) {
    err_info = "TubeMQ Service not startted!";
    return false;
  }
  if (!TubeMQService::Instance()->AddClientObj(err_info, this)) {
    this->status_.CompareAndSet(1, 0);
    return false;
  }
  this->config_ = config;
  this->client_uuid_ = buildUUID();
  this->sub_info_.SetConsumeTarget(this->config_);
  this->rmtdata_cache_.SetConsumerInfo(client_uuid_, config_.GetGroupName());
  // initial resource

  // register to master
  if (!register2Master(err_info, false)) {
    this->status_.CompareAndSet(1, 0);
    return false;
  }
  this->status_.CompareAndSet(1, 2);
  err_info = "Ok";
  return true;
}


void TubeMQConsumer::ShutDown() {
  if (!this->status_.CompareAndSet(2, 0)) {
    return;
  }
  // process resuorce release
}

bool TubeMQConsumer::register2Master(string& err_info, bool need_change) {
  // register function
  return true;
}

bool TubeMQConsumer::buidRegisterRequestC2M(string& err_info,
                                       char** out_msg, int& out_length) {
  string reg_msg;
  RegisterRequestC2M c2m_request;
  list<string>::iterator it_topics;
  list<SubscribeInfo>::iterator it_sub;
  c2m_request.set_clientid(this->client_uuid_);
  c2m_request.set_hostname(TubeMQService::Instance()->GetLocalHost());
  c2m_request.set_requirebound(this->sub_info_.IsBoundConsume());
  c2m_request.set_groupname(this->config_.GetGroupName());
  c2m_request.set_sessiontime(this->sub_info_.GetSubscribedTime());
  // subscribed topic list
  list<string> sub_topics = this->sub_info_.GetSubTopics();
  for (it_topics = sub_topics.begin(); it_topics != sub_topics.end(); ++it_topics) {
    c2m_request.add_topiclist(*it_topics);
  }
  c2m_request.set_defflowcheckid(this->rmtdata_cache_.GetDefFlowCtrlId());
  c2m_request.set_groupflowcheckid(this->rmtdata_cache_.GetGroupFlowCtrlId());
  c2m_request.set_qrypriorityid(this->rmtdata_cache_.GetGroupQryPriorityId());
  // reported subscribed info
  list<SubscribeInfo> subscribe_lst;
  this->rmtdata_cache_.GetSubscribedInfo(subscribe_lst);
  for (it_sub = subscribe_lst.begin(); it_sub != subscribe_lst.end(); ++it_sub) {
    c2m_request.add_subscribeinfo(it_sub->ToString());
  }
  // get topic conditions
  list<string> topic_conds =  this->sub_info_.GetTopicConds();
  for (it_topics = topic_conds.begin(); it_topics != topic_conds.end(); ++it_topics) {
    c2m_request.add_topiccondition(*it_topics);
  }
  // authenticate info
  if (needGenMasterCertificateInfo(true)) {
    MasterCertificateInfo* pmst_certinfo = c2m_request.mutable_authinfo();
    AuthenticateInfo* pauthinfo = pmst_certinfo->mutable_authinfo();
    genMasterAuthenticateToken(pauthinfo,
      config_.GetUsrName(), config_.GetUsrPassWord());
  }
  //
  c2m_request.SerializeToString(&reg_msg);
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
             reg_msg, rpc_config::kMasterMethoddConsumerHeatbeat, serial_no);
  return result;
}


bool TubeMQConsumer::processRegisterResponseM2C(
                              const RegisterResponseM2C& response) {
  return true;
}

bool TubeMQConsumer::buidHeartRequestC2M(string& err_info,
                                   char** out_msg, int& out_length) {
  string hb_msg;
  HeartRequestC2M c2m_request;
  list<string>::iterator it_topics;
  list<SubscribeInfo>::iterator it_sub;
  c2m_request.set_clientid(this->client_uuid_);
  c2m_request.set_groupname(this->config_.GetGroupName());
  c2m_request.set_defflowcheckid(this->rmtdata_cache_.GetDefFlowCtrlId());
  c2m_request.set_groupflowcheckid(this->rmtdata_cache_.GetGroupFlowCtrlId());
  c2m_request.set_qrypriorityid(this->rmtdata_cache_.GetGroupQryPriorityId());
  ConsumerEvent event;
  list<SubscribeInfo>::iterator it;
  list<SubscribeInfo> subscribe_info_lst;
  bool has_event = rmtdata_cache_.PollEventResult(event);
  // judge if report subscribe info
  if ((has_event) || (++cur_report_times_ > config_.GetMaxSubinfoReportIntvl())) {
    cur_report_times_ = 0;
    c2m_request.set_reportsubscribeinfo(true);
    this->rmtdata_cache_.GetSubscribedInfo(subscribe_info_lst);
    if (has_event) {
      EventProto *event_proto = c2m_request.mutable_event();
      event_proto->set_rebalanceid(event.GetRebalanceId());
      event_proto->set_optype(event.GetEventType());
      event_proto->set_status(event.GetEventStatus());
      list<SubscribeInfo> event_sub = event.GetSubscribeInfoList();
      for (it = event_sub.begin(); it != event_sub.end(); it++) {
        event_proto->add_subscribeinfo(it->ToString());
      }
    }
    if (!subscribe_info_lst.empty()) {
      for (it = subscribe_info_lst.begin(); it != subscribe_info_lst.end(); it++) {
        c2m_request.add_subscribeinfo(it->ToString());
      }
    }
  }
  if (needGenMasterCertificateInfo(true)) {
    MasterCertificateInfo* pmst_certinfo = c2m_request.mutable_authinfo();
    AuthenticateInfo* pauthinfo = pmst_certinfo->mutable_authinfo();
    genMasterAuthenticateToken(pauthinfo,
      config_.GetUsrName(), config_.GetUsrPassWord());
  }
  c2m_request.SerializeToString(&hb_msg);
  //
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
                     hb_msg, rpc_config::kMasterMethoddConsumerHeatbeat, serial_no);
  return result;
}

bool TubeMQConsumer::buidCloseRequestC2M(string& err_info,
                                   char** out_msg, int& out_length) {
  string close_msg;
  CloseRequestC2M c2m_request;
  c2m_request.set_clientid(this->client_uuid_);
  c2m_request.set_groupname(this->config_.GetGroupName());
  if (needGenMasterCertificateInfo(true)) {
    MasterCertificateInfo* pmst_certinfo = c2m_request.mutable_authinfo();
    AuthenticateInfo* pauthinfo = pmst_certinfo->mutable_authinfo();
    genMasterAuthenticateToken(pauthinfo,
      config_.GetUsrName(), config_.GetUsrPassWord());
  }
  c2m_request.SerializeToString(&close_msg);
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
              close_msg, rpc_config::kMasterMethoddConsumerClose, serial_no);
  return result;
}

bool TubeMQConsumer::buidRegisterRequestC2B(const PartitionExt& partition, 
                                   string& err_info, char** out_msg, int& out_length) {
  bool is_first_reg;
  int64_t part_offset;
  set<string> filter_cond_set;
  map<string, set<string> > filter_map;
  string register_msg;
  RegisterRequestC2B c2b_request;
  c2b_request.set_clientid(this->client_uuid_);
  c2b_request.set_groupname(this->config_.GetGroupName());
  c2b_request.set_optype(rpc_config::kRegOpTypeRegister);
  c2b_request.set_topicname(partition.GetTopic());
  c2b_request.set_partitionid(partition.GetPartitionId());
  c2b_request.set_qrypriorityid(rmtdata_cache_.GetGroupQryPriorityId());
  is_first_reg = rmtdata_cache_.IsPartitionFirstReg(partition.GetPartitionKey());
  c2b_request.set_readstatus(getConsumeReadStatus(is_first_reg));
  if (sub_info_.IsFilterConsume(partition.GetTopic())) {
    filter_map = sub_info_.GetTopicFilterMap();
    if (filter_map.find(partition.GetTopic()) != filter_map.end()) {
      filter_cond_set = filter_map[partition.GetTopic()];
      for (set<string>::iterator it_cond = filter_cond_set.begin();
        it_cond != filter_cond_set.end(); it_cond++) {
          c2b_request.add_filtercondstr(*it_cond);
      }
    }
  }
  if (is_first_reg 
    && sub_info_.IsBoundConsume() 
    && sub_info_.IsNotAllocated()) {
    sub_info_.GetAssignedPartOffset(partition.GetPartitionKey(), part_offset);
    if (part_offset != tb_config::kInvalidValue) {
      c2b_request.set_curroffset(part_offset);
    }
  }
  AuthorizedInfo* p_authInfo = c2b_request.mutable_authinfo();
  genBrokerAuthenticInfo(p_authInfo, true);
  c2b_request.SerializeToString(&register_msg);
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
              register_msg, rpc_config::kBrokerMethoddConsumerRegister, serial_no);
  return result;
}

bool TubeMQConsumer::buidUnRegRequestC2B(const PartitionExt& partition,
    bool is_last_consumed, string& err_info, char** out_msg, int& out_length) {
  string unreg_msg;
  RegisterRequestC2B c2b_request;
  c2b_request.set_clientid(this->client_uuid_);
  c2b_request.set_groupname(this->config_.GetGroupName());
  c2b_request.set_optype(rpc_config::kRegOpTypeUnReg);
  c2b_request.set_topicname(partition.GetTopic());
  c2b_request.set_partitionid(partition.GetPartitionId());
  c2b_request.set_readstatus(is_last_consumed ? 0 : 1);
  AuthorizedInfo* p_authInfo = c2b_request.mutable_authinfo();
  genBrokerAuthenticInfo(p_authInfo, true);
  c2b_request.SerializeToString(&unreg_msg);
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
              unreg_msg, rpc_config::kBrokerMethoddConsumerRegister, serial_no);
  return result;
}

bool TubeMQConsumer::buidHeartBeatC2B(const list<PartitionExt>& partitions,
  string& err_info, char** out_msg, int& out_length) {
  string hb_msg;
  HeartBeatRequestC2B c2b_request;
  list<PartitionExt>::const_iterator it_part;
  c2b_request.set_clientid(this->client_uuid_);
  c2b_request.set_groupname(this->config_.GetGroupName());
  c2b_request.set_readstatus(getConsumeReadStatus(false));
  c2b_request.set_qrypriorityid(rmtdata_cache_.GetGroupQryPriorityId());
  for (it_part = partitions.begin(); it_part != partitions.end(); ++it_part) {
    c2b_request.add_partitioninfo(it_part->ToString());
  }
  AuthorizedInfo* p_authInfo = c2b_request.mutable_authinfo();
  genBrokerAuthenticInfo(p_authInfo, true);
  c2b_request.SerializeToString(&hb_msg);
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
              hb_msg, rpc_config::kBrokerMethoddConsumerHeatbeat, serial_no);
  return result;
}

bool TubeMQConsumer::buidGetMessageC2B(const PartitionExt& partition,
  bool is_last_consumed, string& err_info, char** out_msg, int& out_length) {
  string get_msg;
  GetMessageRequestC2B c2b_request;
  c2b_request.set_clientid(this->client_uuid_);
  c2b_request.set_groupname(this->config_.GetGroupName());
  c2b_request.set_topicname(partition.GetTopic());
  c2b_request.set_escflowctrl(rmtdata_cache_.IsUnderGroupCtrl());
  c2b_request.set_partitionid(partition.GetPartitionId());
  c2b_request.set_lastpackconsumed(is_last_consumed);
  c2b_request.set_manualcommitoffset(false);
  c2b_request.SerializeToString(&get_msg);
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
              get_msg, rpc_config::kBrokerMethoddConsumerGetMsg, serial_no);
  return result;
}

bool TubeMQConsumer::buidCommitC2B(const PartitionExt& partition,
  bool is_last_consumed, string& err_info, char** out_msg, int& out_length) {
  string commit_msg;
  CommitOffsetRequestC2B c2b_request;
  c2b_request.set_clientid(this->client_uuid_);
  c2b_request.set_groupname(this->config_.GetGroupName());
  c2b_request.set_topicname(partition.GetTopic());
  c2b_request.set_partitionid(partition.GetPartitionId());
  c2b_request.set_lastpackconsumed(is_last_consumed);
  c2b_request.SerializeToString(&commit_msg);
  // begin get serial no from network
  int32_t serial_no = -1;
  // end get serial no from network
  bool result = getSerializedMsg(err_info, out_msg, out_length,
              commit_msg, rpc_config::kBrokerMethoddConsumerCommit, serial_no);
  return result;
}

bool TubeMQConsumer::getSerializedMsg(string& err_info,
  char** out_msg, int& out_length, const string& req_msg,
  const int32_t method_id, int32_t serial_no) {
  //
  RequestBody req_body;
  req_body.set_method(method_id);
  req_body.set_timeout(this->config_.GetRpcReadTimeoutMs());
  req_body.set_request(req_msg);
  RequestHeader req_header;
  req_header.set_servicetype(Utils::GetServiceTypeByMethodId(method_id));
  req_header.set_protocolver(2);
  RpcConnHeader rpc_header;
  rpc_header.set_flag(rpc_config::kRpcFlagMsgRequest);
  // process serial
  return true;
}



string TubeMQConsumer::buildUUID() {
  stringstream ss;
  ss << this->config_.GetGroupName();
  ss << "_";
  ss << TubeMQService::Instance()->GetLocalHost();
  ss << "-";
  ss << getpid();
  ss << "-";
  ss << Utils::GetCurrentTimeMillis();
  ss << "-";
  ss << GetClientIndex();
  ss << "-";
  ss << kTubeMQClientVersion;
  return ss.str();
}

int32_t TubeMQConsumer::getConsumeReadStatus(bool is_first_reg) {
  int32_t readStatus = rpc_config::kConsumeStatusNormal;
  if (is_first_reg) {
    if(this->config_.GetConsumePosition() == 0) {
      readStatus = rpc_config::kConsumeStatusFromMax;
      LOG_INFO("[Consumer From Max Offset], clientId=%s",
        this->client_uuid_.c_str());
    } else if(this->config_.GetConsumePosition() > 0) {
      readStatus = rpc_config::kConsumeStatusFromMaxAlways;
      LOG_INFO("[Consumer From Max Offset Always], clientId=%s",
        this->client_uuid_.c_str());
    }
  }
  return readStatus;
}

bool TubeMQConsumer::needGenMasterCertificateInfo(bool force) {
  bool needAdd = false;
  if (config_.IsAuthenticEnabled()) {
    if (force) {
      needAdd = true;
      nextauth_2_master.Set(false);
    } else if (nextauth_2_master.Get()) {
      if (nextauth_2_master.CompareAndSet(true, false)) {
        needAdd = true;
      }      
    }
  }
  return needAdd;
}

void TubeMQConsumer::genBrokerAuthenticInfo(AuthorizedInfo* p_authInfo, bool force) {
  bool needAdd = false;
  p_authInfo->set_visitauthorizedtoken(visit_token_.Get());
  if (config_.IsAuthenticEnabled()) {
    if (force) {
      needAdd = true;
      nextauth_2_broker.Set(false);
    } else if (nextauth_2_broker.Get()) {
      if (nextauth_2_broker.CompareAndSet(true, false)) {
        needAdd = true;
      }
    }
    if (needAdd) {
      string auth_token = Utils::GenBrokerAuthenticateToken(
        config_.GetUsrName(), config_.GetUsrPassWord());
      p_authInfo->set_authauthorizedtoken(auth_token);
    }
  }
}

void TubeMQConsumer::genMasterAuthenticateToken(AuthenticateInfo* pauthinfo,
  const string& username, const string usrpassword) {
  //
}


}  // namespace tubemq

