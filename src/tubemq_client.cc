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

#include <unistd.h>
#include <signal.h>
#include <sstream>
#include "tubemq/client_service.h"
#include "tubemq/utils.h"
#include "tubemq/version.h"
#include "RPC.pb.h"
#include "MasterService.pb.h"
#include "BrokerService.pb.h"




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
  client_uuid_ = "";
}

TubeMQConsumer::~TubeMQConsumer() {

}

bool TubeMQConsumer::Start(string& err_info, const ConsumerConfig& config) {
  bool result;
  ConsumerConfig tmp_config;
  
  if(!this->status_.CompareAndSet(0, 1)) {
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
  if(!register2Master(err_info, false)) {
    this->status_.CompareAndSet(1, 0);
    return false;        
  }
  this->status_.CompareAndSet(1, 2);
  err_info = "Ok";
  return true;  
}


void TubeMQConsumer::ShutDown() {
  if (!this->status_.CompareAndSet(2, 0)) {
    err_info = "Ok";
    return true;
  }
  // process resuorce release
}




bool TubeMQConsumer::register2Master(string& err_info, bool need_change) {
  // register function
}

RegisterRequestC2M TubeMQConsumer::buidRegisterRequestC2M() {
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
    c2m_request.add_subscribeinfo(*it_sub);
  }
  // get topic conditions
  list<string> topic_conds =  this->sub_info_.GetTopicConds();
  for (it_topics = topic_conds.begin(); it_topics != topic_conds.end(); ++it_topics) {
    c2m_request.add_topiccondition(*it_topics);
  }
  // authenticate info
  // end
  return c2m_request;
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
  ss << this->GetClientIndex();
  ss << "-";
  ss << kTubeMQClientVersion;
  return ss.str();
}



}  // namespace tubemq

