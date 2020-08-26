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
#include "tubemq/singleton.h"
#include "tubemq/transport.h"
#include "tubemq/tubemq_transport.h"
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
  curr_master_addr_ = "";
  masters_map_.clear();
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
  if (config.GetGroupName().length() == 0 || config.GetMasterAddrInfo().length() == 0) {
    err_info = "Parameter error: not set master address info or group name!";
    return false;
  }
  //
  if (!TubeMQService::Instance()->IsRunning()) {
    err_info = "TubeMQ Service not startted!";
    return false;
  }
  if (!TubeMQService::Instance()->AddClientObj(err_info, this, client_index_)) {
    client_index_ = tb_config::kInvalidValue;
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
  TubeMQService::Instance()->RmvClientObj(client_index_);
  client_index_ = tb_config::kInvalidValue;
  // process resuorce release
}

bool TubeMQConsumer::register2Master(string& err_info, bool need_change) {
  string target_ip;
  int target_port;
  // check client status
  if (this->status_.Get() == 0) {
    err_info = "Consumer not startted!";
    return false;
  }

  LOG_DEBUG("[REGISTER], initial register request, clientId= %s", this->client_uuid_.c_str());
  // get master address and port
  if (need_change) {
    getNextMasterAddr(target_ip, target_port);
  } else {
    getCurrentMasterAddr(target_ip, target_port);
  }
  bool result = false;
  int retry_count = 0;
  int maxRetrycount = masters_map_.size();
  err_info = "Master register failure, no online master service!";
  while (retry_count < maxRetrycount) {
    if (!TubeMQService::Instance()->IsRunning()) {
      err_info = "TubeMQ Service not stopped!";
      LOG_INFO("[REGISTER] register2Master failure, %s", err_info.c_str());
      return false;
    }
    RequestContextPtr request;
    TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
    // build register request
    buidRegisterRequestC2M(req_protocol);
    // set parameters
    request->ip_ = target_ip;
    request->port_ = target_port;
    request->timeout_ = config_.GetRpcReadTimeoutMs();
    request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
    req_protocol->request_id_ = request->request_id_;
    req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
    // send message to target
    ResponseContext response_context;
    ErrorCode error = SyncRequest(response_context, request, req_protocol);
    // process response
    auto rsp = any_cast<TubeMQCodec::RspProtocolPtr>(response_context.rsp_);
    result = processRegisterResponseM2C(err_info, rsp);
    if (result) {
      err_info = "Ok";
      break;
    }
    LOG_WARN("[REGISTER] register to (%s:%d) failure, retrycount=(%d-%d), reason is %s",
      target_ip.c_str(), target_port, maxRetrycount, retry_count+1, err_info.c_str());
    retry_count++;
    getNextMasterAddr(target_ip, target_port);
  }
  return result;
}

void TubeMQConsumer::buidRegisterRequestC2M(TubeMQCodec::ReqProtocolPtr& req_protocol) {
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
  list<string> topic_conds = this->sub_info_.GetTopicConds();
  for (it_topics = topic_conds.begin(); it_topics != topic_conds.end(); ++it_topics) {
    c2m_request.add_topiccondition(*it_topics);
  }
  // authenticate info
  if (needGenMasterCertificateInfo(true)) {
    MasterCertificateInfo* pmst_certinfo = c2m_request.mutable_authinfo();
    AuthenticateInfo* pauthinfo = pmst_certinfo->mutable_authinfo();
    genMasterAuthenticateToken(pauthinfo, config_.GetUsrName(), config_.GetUsrPassWord());
  }
  //
  c2m_request.SerializeToString(&reg_msg);
  req_protocol->method_id_ = rpc_config::kMasterMethoddConsumerRegister;
  req_protocol->prot_msg_ = reg_msg;
}

void TubeMQConsumer::buidHeartRequestC2M(TubeMQCodec::ReqProtocolPtr& req_protocol) {
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
      EventProto* event_proto = c2m_request.mutable_event();
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
    genMasterAuthenticateToken(pauthinfo, config_.GetUsrName(), config_.GetUsrPassWord());
  }
  c2m_request.SerializeToString(&hb_msg);
  req_protocol->method_id_ = rpc_config::kMasterMethoddConsumerHeatbeat;
  req_protocol->prot_msg_ = hb_msg;
}

void TubeMQConsumer::buidCloseRequestC2M(TubeMQCodec::ReqProtocolPtr& req_protocol) {
  string close_msg;
  CloseRequestC2M c2m_request;
  c2m_request.set_clientid(this->client_uuid_);
  c2m_request.set_groupname(this->config_.GetGroupName());
  if (needGenMasterCertificateInfo(true)) {
    MasterCertificateInfo* pmst_certinfo = c2m_request.mutable_authinfo();
    AuthenticateInfo* pauthinfo = pmst_certinfo->mutable_authinfo();
    genMasterAuthenticateToken(pauthinfo, config_.GetUsrName(), config_.GetUsrPassWord());
  }
  c2m_request.SerializeToString(&close_msg);
  req_protocol->method_id_ = rpc_config::kMasterMethoddConsumerClose;
  req_protocol->prot_msg_ = close_msg;
}

void TubeMQConsumer::buidRegisterRequestC2B(const PartitionExt& partition,
                                            TubeMQCodec::ReqProtocolPtr& req_protocol) {
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
  if (is_first_reg && sub_info_.IsBoundConsume() && sub_info_.IsNotAllocated()) {
    sub_info_.GetAssignedPartOffset(partition.GetPartitionKey(), part_offset);
    if (part_offset != tb_config::kInvalidValue) {
      c2b_request.set_curroffset(part_offset);
    }
  }
  AuthorizedInfo* p_authInfo = c2b_request.mutable_authinfo();
  genBrokerAuthenticInfo(p_authInfo, true);
  c2b_request.SerializeToString(&register_msg);
  req_protocol->method_id_ = rpc_config::kBrokerMethoddConsumerRegister;
  req_protocol->prot_msg_ = register_msg;
}

void TubeMQConsumer::buidUnRegRequestC2B(const PartitionExt& partition, bool is_last_consumed,
                                         TubeMQCodec::ReqProtocolPtr& req_protocol) {
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
  req_protocol->method_id_ = rpc_config::kBrokerMethoddConsumerRegister;
  req_protocol->prot_msg_ = unreg_msg;
}

void TubeMQConsumer::buidHeartBeatC2B(const list<PartitionExt>& partitions,
                                      TubeMQCodec::ReqProtocolPtr& req_protocol) {
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
  req_protocol->method_id_ = rpc_config::kBrokerMethoddConsumerHeatbeat;
  req_protocol->prot_msg_ = hb_msg;
}

void TubeMQConsumer::buidGetMessageC2B(const PartitionExt& partition, bool is_last_consumed,
                                       TubeMQCodec::ReqProtocolPtr& req_protocol) {
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
  req_protocol->method_id_ = rpc_config::kBrokerMethoddConsumerGetMsg;
  req_protocol->prot_msg_ = get_msg;
}

void TubeMQConsumer::buidCommitC2B(const PartitionExt& partition, bool is_last_consumed,
                                   TubeMQCodec::ReqProtocolPtr& req_protocol) {
  string commit_msg;
  CommitOffsetRequestC2B c2b_request;
  c2b_request.set_clientid(this->client_uuid_);
  c2b_request.set_groupname(this->config_.GetGroupName());
  c2b_request.set_topicname(partition.GetTopic());
  c2b_request.set_partitionid(partition.GetPartitionId());
  c2b_request.set_lastpackconsumed(is_last_consumed);
  c2b_request.SerializeToString(&commit_msg);
  req_protocol->method_id_ = rpc_config::kBrokerMethoddConsumerCommit;
  req_protocol->prot_msg_ = commit_msg;
}

bool TubeMQConsumer::processRegisterResponseM2C(string& err_info,
  const TubeMQCodec::RspProtocolPtr& rsp_protocol) {
  
  
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
    if (this->config_.GetConsumePosition() == 0) {
      readStatus = rpc_config::kConsumeStatusFromMax;
      LOG_INFO("[Consumer From Max Offset], clientId=%s", this->client_uuid_.c_str());
    } else if (this->config_.GetConsumePosition() > 0) {
      readStatus = rpc_config::kConsumeStatusFromMaxAlways;
      LOG_INFO("[Consumer From Max Offset Always], clientId=%s", this->client_uuid_.c_str());
    }
  }
  return readStatus;
}

bool TubeMQConsumer::initMasterAddress(string& err_info, const string& master_info) {
  masters_map_.clear();
  Utils::Split(master_info, masters_map_, delimiter::kDelimiterComma, delimiter::kDelimiterColon);
  if (masters_map_.empty()) {
    err_info = "Illegal parameter: master_info is blank!";
    return false;
  }
  bool needXfs = false;
  map<string, int32_t>::iterator it;
  for (it = masters_map_.begin(); it != masters_map_.end(); it++) {
    if (Utils::NeedDnsXfs(it->first)) {
      needXfs = true;
      break;
    }
  }
  it = masters_map_.begin();
  curr_master_addr_ = it->first;
  if (needXfs) {
    TubeMQService::Instance()->AddMasterAddress(err_info, master_info);
  }
  err_info = "Ok";
  return true;
}

void TubeMQConsumer::getNextMasterAddr(string& ipaddr, int32_t& port) {
  map<string, int32_t>::iterator it;
  it = masters_map_.find(curr_master_addr_);
  if (it != masters_map_.end()) {
    it++;
    if (it == masters_map_.end()) {
      it = masters_map_.begin();
    }
  } else {
    it = masters_map_.begin();
  }
  ipaddr = it->first;
  port = it->second;
  curr_master_addr_ = it->first;
  if (Utils::NeedDnsXfs(ipaddr)) {
    TubeMQService::Instance()->GetXfsMasterAddress(curr_master_addr_, ipaddr);
  }
}

void TubeMQConsumer::getCurrentMasterAddr(string& ipaddr, int32_t& port) {
  ipaddr = curr_master_addr_;
  port = masters_map_[curr_master_addr_];
  if (Utils::NeedDnsXfs(ipaddr)) {
    TubeMQService::Instance()->GetXfsMasterAddress(curr_master_addr_, ipaddr);
  }
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
      string auth_token =
          Utils::GenBrokerAuthenticateToken(config_.GetUsrName(), config_.GetUsrPassWord());
      p_authInfo->set_authauthorizedtoken(auth_token);
    }
  }
}

void TubeMQConsumer::genMasterAuthenticateToken(AuthenticateInfo* pauthinfo, const string& username,
                                                const string usrpassword) {
  //
}

}  // namespace tubemq

