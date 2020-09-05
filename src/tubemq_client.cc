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
#include "tubemq/tubemq_config.h"
#include "tubemq/tubemq_errcode.h"
#include "tubemq/tubemq_transport.h"
#include "tubemq/utils.h"
#include "tubemq/version.h"

namespace tubemq {

using std::lock_guard;
using std::stringstream;

bool StartTubeMQService(string& err_info, const string& conf_file) {
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
  unreport_times_ = 0;
  visit_token_.Set(tb_config::kInvalidValue);
  nextauth_2_master.Set(false);
  nextauth_2_broker.Set(false);
  masters_map_.clear();
  is_master_actived_ = false;
  last_master_hbtime_ = 0;
  master_sh_retry_cnt_ = 0;
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
    status_.CompareAndSet(1, 0);
    return false;
  }
  config_ = config;
  if (!initMasterAddress(err_info, config.GetMasterAddrInfo())) {
    return false;
  }
  client_uuid_ = buildUUID();
  sub_info_.SetConsumeTarget(this->config_);
  rmtdata_cache_.SetConsumerInfo(client_uuid_, config_.GetGroupName());
  // initial resource

  // register to master
  int32_t error_code;
  if (!register2Master(error_code, err_info, false)) {
    status_.CompareAndSet(1, 0);
    return false;
  }
  status_.CompareAndSet(1, 2);
  heart_beat_timer_ = TubeMQService::Instance()->CreateTimer();
  heart_beat_timer_->expires_after(std::chrono::milliseconds(config_.GetHeartbeatPeriodMs()));
  heart_beat_timer_->async_wait([this](const std::error_code& ec) { heartBeat2Master(); });
  rebalance_thread_ptr_ = std::make_shared<std::thread>([this]() { processRebalanceEvent(); });

  err_info = "Ok";
  return true;
}

void TubeMQConsumer::ShutDown() {
  if (status_.CompareAndSet(2, 0)) {
    return;
  }
  // exist rebalance thread
  ConsumerEvent empty_event;
  rmtdata_cache_.OfferEvent(empty_event);
  // remove client stub
  TubeMQService::Instance()->RmvClientObj(client_index_);
  client_index_ = tb_config::kInvalidValue;
  heart_beat_timer_ = nullptr;
  rebalance_thread_ptr_->join();
  rebalance_thread_ptr_ = nullptr;
  // process resuorce release
}

bool TubeMQConsumer::GetMessage(ConsumerResult& result) {
  if (!TubeMQService::Instance()->IsRunning()) {
    result.SetFailureResult(err_code::kErrServerStop, "TubeMQ Service stopped!");
    return false;
  }
  if (!isClientRunning()) {
    result.SetFailureResult(err_code::kErrClientStop, "TubeMQ Client stopped!");
    return false;
  }
  int32_t error_code;
  string err_info;
  PartitionExt partition_ext;
  string confirm_context;
  if (!rmtdata_cache_.SelectPartition(error_code, err_info, partition_ext, confirm_context)) {
    result.SetFailureResult(error_code, err_info);
    return false;
  }
  long curr_offset = tb_config::kInvalidValue;
  bool filter_consume = sub_info_.IsFilterConsume(partition_ext.GetTopic());
  PeerInfo peer_info(partition_ext, curr_offset);
  auto request = std::make_shared<RequestContext>();
  TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
  // build getmessage request
  buidGetMessageC2B(partition_ext, partition_ext.IsLastConsumed(), req_protocol);
  request->codec_ = std::make_shared<TubeMQCodec>();
  request->ip_ = partition_ext.GetBrokerHost();
  request->port_ = partition_ext.GetBrokerPort();
  request->timeout_ = config_.GetRpcReadTimeoutMs();
  request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
  req_protocol->request_id_ = request->request_id_;
  req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
  // send message to target
  ResponseContext response_context;
  ErrorCode error = SyncRequest(response_context, request, req_protocol);
  if (error.Value() == err_code::kErrSuccess) {
    // process response
    auto rsp = any_cast<TubeMQCodec::RspProtocolPtr>(response_context.rsp_);
    processGetMessageRspB2C(result, peer_info, filter_consume, partition_ext, confirm_context, rsp);
    return result.IsSuccess();
  } else {
    rmtdata_cache_.RelPartition(err_info, filter_consume, confirm_context, false);
    result.SetFailureResult(error.Value(), error.Message(), partition_ext.GetTopic(), peer_info);
    return false;
  }
}

bool TubeMQConsumer::Confirm(const string& confirm_context, bool is_consumed,
                             ConsumerResult& result) {
  if (!TubeMQService::Instance()->IsRunning()) {
    result.SetFailureResult(err_code::kErrServerStop, "TubeMQ Service stopped!");
    return false;
  }
  if (!isClientRunning()) {
    result.SetFailureResult(err_code::kErrClientStop, "TubeMQ Client stopped!");
    return false;
  }
  string token1 = delimiter::kDelimiterAt;
  string token2 = delimiter::kDelimiterColon;
  string::size_type pos1, pos2;
  pos1 = confirm_context.find(token1);
  if (string::npos == pos1) {
    result.SetFailureResult(
        err_code::kErrBadRequest,
        "Illegel confirm_context content: unregular confirm_context value format!");
    return false;
  }
  string part_key = Utils::Trim(confirm_context.substr(0, pos1));
  string booked_time_str =
      Utils::Trim(confirm_context.substr(pos1 + token1.size(), confirm_context.size()));
  long booked_time = atol(booked_time_str.c_str());
  pos1 = part_key.find(token2);
  if (string::npos == pos1) {
    result.SetFailureResult(err_code::kErrBadRequest,
                            "Illegel confirm_context content: unregular index key value format!");
    return false;
  }
  pos1 = pos1 + token1.size();
  string topic_name = part_key.substr(pos1);
  pos2 = topic_name.rfind(token2);
  if (string::npos == pos2) {
    result.SetFailureResult(
        err_code::kErrBadRequest,
        "Illegel confirm_context content: unregular index's topic key value format!");
    return false;
  }
  topic_name = topic_name.substr(0, pos2);
  if (!rmtdata_cache_.IsPartitionInUse(part_key, booked_time)) {
    result.SetFailureResult(err_code::kErrConfirmTimeout, "The confirm_context's value invalid!");
    return false;
  }
  PartitionExt partition_ext;
  bool ret_result = rmtdata_cache_.GetPartitionExt(part_key, partition_ext);
  if (!ret_result) {
    result.SetFailureResult(err_code::kErrConfirmTimeout,
                            "Not found the partition by confirm_context!");
    return false;
  }
  long curr_offset = tb_config::kInvalidValue;
  PeerInfo peer_info(partition_ext, curr_offset);
  auto request = std::make_shared<RequestContext>();
  TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
  // build CommitC2B request
  buidCommitC2B(partition_ext, sub_info_.IsFilterConsume(topic_name), req_protocol);
  request->codec_ = std::make_shared<TubeMQCodec>();
  request->ip_ = partition_ext.GetBrokerHost();
  request->port_ = partition_ext.GetBrokerPort();
  request->timeout_ = config_.GetRpcReadTimeoutMs();
  request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
  req_protocol->request_id_ = request->request_id_;
  req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
  // send message to target
  ResponseContext response_context;
  ErrorCode error = SyncRequest(response_context, request, req_protocol);
  if (error.Value() == err_code::kErrSuccess) {
    // process response
    auto rsp = any_cast<TubeMQCodec::RspProtocolPtr>(response_context.rsp_);
    if (rsp->success_) {
      CommitOffsetResponseB2C rsp_b2c;
      ret_result = rsp_b2c.ParseFromArray(rsp->rsp_body_.data().c_str(),
                                          (int)(rsp->rsp_body_.data().length()));
      if (ret_result) {
        if (rsp_b2c.success()) {
          curr_offset = rsp_b2c.curroffset();
          peer_info.SetCurrOffset(curr_offset);
          result.SetSuccessResult(err_code::kErrSuccess, topic_name, peer_info);
        } else {
          result.SetFailureResult(rsp_b2c.errcode(), rsp_b2c.errmsg(), topic_name, peer_info);
        }
      } else {
        result.SetFailureResult(err_code::kErrParseFailure,
                                "Parse CommitOffsetResponseB2C response failure!", topic_name,
                                peer_info);
      }
    } else {
      result.SetFailureResult(rsp->code_, rsp->error_msg_, topic_name, peer_info);
    }
  } else {
    result.SetFailureResult(error.Value(), error.Message(), topic_name, peer_info);
  }
  string err_info;
  rmtdata_cache_.BookedPartionInfo(part_key, curr_offset);
  rmtdata_cache_.RelPartition(err_info, sub_info_.IsFilterConsume(topic_name), confirm_context,
                              is_consumed);
  return result.IsSuccess();
}

bool TubeMQConsumer::register2Master(int32_t& error_code, string& err_info, bool need_change) {
  string target_ip;
  int target_port;
  // check client status
  if (status_.Get() == 0) {
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
      err_info = "TubeMQ Service is stopped!";
      LOG_INFO("[REGISTER] register2Master failure, %s", err_info.c_str());
      return false;
    }
    auto request = std::make_shared<RequestContext>();
    TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
    // build register request
    buidRegisterRequestC2M(req_protocol);
    // set parameters
    request->codec_ = std::make_shared<TubeMQCodec>();
    request->ip_ = target_ip;
    request->port_ = target_port;
    request->timeout_ = config_.GetRpcReadTimeoutMs();
    request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
    req_protocol->request_id_ = request->request_id_;
    req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
    // send message to target
    ResponseContext response_context;
    ErrorCode error = SyncRequest(response_context, request, req_protocol);
    if (error.Value() == err_code::kErrSuccess) {
      // process response
      auto rsp = any_cast<TubeMQCodec::RspProtocolPtr>(response_context.rsp_);
      result = processRegisterResponseM2C(error_code, err_info, rsp);
      if (result) {
        err_info = "Ok";
        is_master_actived_ = true;
        last_master_hbtime_ = Utils::GetCurrentTimeMillis();
        break;
      } else {
        is_master_actived_ = false;
      }
    } else {
      error_code = error.Value();
      err_info = error.Message();
    }
    if (error_code == err_code::kErrConsumeGroupForbidden ||
        error_code == err_code::kErrConsumeContentForbidden) {
      LOG_WARN("[REGISTER] register to (%s:%d) failure, reason is %s, exist register process",
               target_ip.c_str(), target_port, err_info.c_str());
      return false;
    } else {
      LOG_WARN("[REGISTER] register to (%s:%d) failure, retrycount=(%d-%d), reason is %s",
               target_ip.c_str(), target_port, maxRetrycount, retry_count + 1, err_info.c_str());
    }
    retry_count++;
    getNextMasterAddr(target_ip, target_port);
  }
  return result;
}

void TubeMQConsumer::asyncRegister2Master(bool need_change) {
  TubeMQService::Instance()->Post([this, need_change]() {
    int32_t error_code;
    string error_info;
    auto ret_result = register2Master(error_code, error_info, need_change);
    LOG_INFO("asyncRegister2Master ret_result:%d, master_sh_retry_cnt_:%d", ret_result,
             master_sh_retry_cnt_);
    if (ret_result) {
      is_master_actived_ = true;
      master_sh_retry_cnt_ = 0;
    } else {
      master_sh_retry_cnt_++;
    }
    heart_beat_timer_->expires_after(std::chrono::milliseconds(nextHeartBeatPeriodms()));
    heart_beat_timer_->async_wait([this](const std::error_code& ec) { heartBeat2Master(); });
  });
}

void TubeMQConsumer::heartBeat2Master() {
  // timer task
  // 1. check if need re-register, if true, first call register
  // 2. call heartbeat to master
  // 3. process response
  // 4. call timer again
  string target_ip;
  int target_port;

  heart_beat_timer_->cancel();
  if (!TubeMQService::Instance()->IsRunning()) {
    LOG_INFO("[HeartBeat2Master] heartBeat2Master failure, TubeMQ Service is stopped!");
    return;
  }

  if (!isClientRunning()) {
    LOG_INFO("[HeartBeat2Master] heartBeat2Master failure, TubeMQ Client stopped!");
    return;
  }

  // check status in master
  // if not actived first register, or send heartbeat
  if (is_master_actived_ == false) {
    asyncRegister2Master(false);
    return;
  }
  // select current master
  getCurrentMasterAddr(target_ip, target_port);
  auto request = std::make_shared<RequestContext>();
  TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
  // build heartbeat 2 master request
  buidHeartRequestC2M(req_protocol);
  request->codec_ = std::make_shared<TubeMQCodec>();
  request->ip_ = target_ip;
  request->port_ = target_port;
  request->timeout_ = config_.GetRpcReadTimeoutMs();
  request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
  req_protocol->request_id_ = request->request_id_;
  req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
  // send message to target
  AsyncRequest(request, req_protocol)
      .AddCallBack([=](ErrorCode error, const ResponseContext& response_context) {
        if (error.Value() != err_code::kErrSuccess) {
          master_sh_retry_cnt_++;
          LOG_WARN("[HeartBeat2Master] request network failue to (%s:%d) : %s", target_ip.c_str(),
                   target_port, error.Message().c_str());
        } else {
          // process response
          auto rsp = any_cast<TubeMQCodec::RspProtocolPtr>(response_context.rsp_);
          int32_t error_code = 0;
          std::string error_info;
          auto ret_result = processHBResponseM2C(error_code, error_info, rsp);
          if (ret_result) {
            is_master_actived_ = true;
            master_sh_retry_cnt_ = 0;
          } else {
            master_sh_retry_cnt_++;
            if (error_code == err_code::kErrHbNoNode ||
                error_info.find("StandbyException") != string::npos) {
              is_master_actived_ = false;
              asyncRegister2Master(!(error_code == err_code::kErrHbNoNode));
              LOG_INFO("heartbeat no node or standby exception. retry to do register2master");
              return;
            }
          }
        }
        heart_beat_timer_->expires_after(std::chrono::milliseconds(nextHeartBeatPeriodms()));
        heart_beat_timer_->async_wait([this](const std::error_code& ec) { heartBeat2Master(); });
      });
  return;
}

int32_t TubeMQConsumer::nextHeartBeatPeriodms() {
  int32_t next_hb_periodms = config_.GetHeartbeatPeriodMs();
  if (master_sh_retry_cnt_ >= config_.GetMaxHeartBeatRetryTimes()) {
    next_hb_periodms = config_.GetHeartbeatPeriodAftFailMs();
  }
  return next_hb_periodms;
}

void TubeMQConsumer::processRebalanceEvent() {
  // thread wait until event come
  LOG_INFO("begin, rebalance event Handler start");
  while (true) {
    if (!TubeMQService::Instance()->IsRunning()) {
      LOG_INFO("[Rebalance Event] TubeMQ Service is stopped, RebalanceEvent existed!");
      break;
    }
    if (!isClientRunning()) {
      LOG_INFO("[Rebalance Event] TubeMQ Client is stopped, RebalanceEvent existed!");
      break;
    }
    ConsumerEvent event;
    rmtdata_cache_.TakeEvent(event);
    if (event.GetEventStatus() == tb_config::kInvalidValue &&
        event.GetRebalanceId() == tb_config::kInvalidValue) {
      LOG_INFO("[Rebalance Event]  found existed ,out RebalanceEvent Task handler!");
      break;
    }
    rmtdata_cache_.ClearEvent();
    switch (event.GetEventType()) {
      case 2:
      case 20: {
        processDisConnect2Broker(event);
        rmtdata_cache_.OfferEventResult(event);
      } break;

      case 1:
      case 10: {
        processConnect2Broker(event);
        rmtdata_cache_.OfferEventResult(event);
      } break;

      default: {
        //
      } break;
    }
  }

  LOG_INFO("end, rebalance event Handler stop");
  return;
}

void TubeMQConsumer::close2Master() {
  string target_ip;
  int target_port;
  if (!isClientRunning()) {
    LOG_INFO("Client not running ,out close2Master\n");
    return;
  }
  getCurrentMasterAddr(target_ip, target_port);
  auto request = std::make_shared<RequestContext>();
  TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
  // build close2master request
  buidCloseRequestC2M(req_protocol);
  request->codec_ = std::make_shared<TubeMQCodec>();
  request->ip_ = target_ip;
  request->port_ = target_port;
  request->timeout_ = config_.GetRpcReadTimeoutMs();
  request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
  req_protocol->request_id_ = request->request_id_;
  req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
  // send message to target
  AsyncRequest(request, req_protocol);
  // not need wait response
  return;
}

void TubeMQConsumer::processConnect2Broker(ConsumerEvent& event) {
  if (!isClientRunning()) {
    return;
  }
  bool ret_result;
  int32_t error_code;
  string error_info;
  list<PartitionExt> subscribed_partitions;
  list<PartitionExt> unsub_partitions;
  list<PartitionExt>::iterator it;
  list<SubscribeInfo> subscribe_info = event.GetSubscribeInfoList();
  if (!subscribe_info.empty()) {
    rmtdata_cache_.FilterPartitions(subscribe_info, subscribed_partitions, unsub_partitions);
    if (!unsub_partitions.empty()) {
      for (it = unsub_partitions.begin(); it != unsub_partitions.end(); it++) {
        auto request = std::make_shared<RequestContext>();
        TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
        // build close2master request
        buidRegisterRequestC2B(*it, req_protocol);
        request->codec_ = std::make_shared<TubeMQCodec>();
        request->ip_ = it->GetBrokerHost();
        request->port_ = it->GetBrokerPort();
        request->timeout_ = config_.GetRpcReadTimeoutMs();
        request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
        req_protocol->request_id_ = request->request_id_;
        req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
        // send message to target
        ResponseContext response_context;
        ErrorCode error = SyncRequest(response_context, request, req_protocol);
        if (error.Value() != err_code::kErrSuccess) {
          LOG_WARN("[Connect2Broker] request network failure to (%s:%d) : %s",
                   it->GetBrokerHost().c_str(), it->GetBrokerPort(), error.Message().c_str());
        } else {
          // process response
          auto rsp = any_cast<TubeMQCodec::RspProtocolPtr>(response_context.rsp_);
          ret_result = processRegResponseB2C(error_code, error_info, rsp);
          if (ret_result) {
            rmtdata_cache_.AddNewPartition(*it);
            // TODO: add hearbeat timer by broker IP?
          }
        }
      }
    }
  }
  event.SetEventStatus(2);
}

void TubeMQConsumer::processDisConnect2Broker(ConsumerEvent& event) {
  if (!isClientRunning()) {
    return;
  }
  list<SubscribeInfo> subscribe_info = event.GetSubscribeInfoList();
  if (!subscribe_info.empty()) {
    map<NodeInfo, list<PartitionExt> > rmv_partitions;
    rmtdata_cache_.RemoveAndGetPartition(subscribe_info, config_.IsRollbackIfConfirmTimeout(),
                                         rmv_partitions);
    if (!rmv_partitions.empty()) {
      unregister2Brokers(rmv_partitions, true);
    }
  }
  event.SetEventStatus(2);
  return;
}

void TubeMQConsumer::processHeartBeat2Broker(NodeInfo broker_info) {
  if (!isClientRunning()) {
    return;
  }
  list<PartitionExt> partition_list;
  list<PartitionExt>::iterator it;
  rmtdata_cache_.GetPartitionByBroker(broker_info, partition_list);
  if (partition_list.empty()) {
    // TODO: delete hearbeat timer by broker IP?
    return;
  }
  auto request = std::make_shared<RequestContext>();
  TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
  // build heartbeat2broker request
  buidHeartBeatC2B(partition_list, req_protocol);
  request->codec_ = std::make_shared<TubeMQCodec>();
  request->ip_ = broker_info.GetHost();
  request->port_ = broker_info.GetPort();
  request->timeout_ = config_.GetRpcReadTimeoutMs();
  request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
  req_protocol->request_id_ = request->request_id_;
  req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
  // send message to target
  ResponseContext response_context;
  ErrorCode error = SyncRequest(response_context, request, req_protocol);
  if (error.Value() != err_code::kErrSuccess) {
    LOG_WARN("[Heartbeat2Broker] request network  to failure (%s:%d) : %s",
             broker_info.GetHost().c_str(), broker_info.GetPort(), error.Message().c_str());
  } else {
    // process response
    auto rsp = any_cast<TubeMQCodec::RspProtocolPtr>(response_context.rsp_);
    if (rsp->success_) {
      HeartBeatResponseB2C rsp_b2c;
      bool result = rsp_b2c.ParseFromArray(rsp->rsp_body_.data().c_str(),
                                           (int)(rsp->rsp_body_.data().length()));
      if (result) {
        set<string> partition_keys;
        if (rsp_b2c.has_haspartfailure() && rsp_b2c.haspartfailure()) {
          for (int tmp_i = 0; tmp_i < rsp_b2c.failureinfo_size(); tmp_i++) {
            string token_key = delimiter::kDelimiterColon;
            string fullpart_str = rsp_b2c.failureinfo(tmp_i);
            string::size_type pos1 = fullpart_str.find(token_key);
            if (pos1 == string::npos) {
              continue;
            }
            // int error_code = atoi(fullpart_str.substr(0, pos1).c_str());
            string part_str = fullpart_str.substr(pos1 + token_key.size(), fullpart_str.size());
            Partition part(part_str);
            partition_keys.insert(part.GetPartitionKey());
          }
        }
        rmtdata_cache_.RemovePartition(partition_keys);
      }
    }
  }
  // TODO: add hearbeat timer by broker IP?
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
  if ((has_event) || (++unreport_times_ > config_.GetMaxSubinfoReportIntvl())) {
    unreport_times_ = 0;
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

bool TubeMQConsumer::processRegisterResponseM2C(int32_t& error_code, string& err_info,
                                                const TubeMQCodec::RspProtocolPtr& rsp_protocol) {
  if (!rsp_protocol->success_) {
    error_code = rsp_protocol->code_;
    err_info = rsp_protocol->error_msg_;
    return false;
  }
  RegisterResponseM2C rsp_m2c;
  bool result = rsp_m2c.ParseFromArray(rsp_protocol->rsp_body_.data().c_str(),
                                       (int)(rsp_protocol->rsp_body_.data().length()));
  if (!result) {
    error_code = err_code::kErrParseFailure;
    err_info = "Parse RegisterResponseM2C response failure!";
    return false;
  }
  if (!rsp_m2c.success()) {
    error_code = rsp_m2c.errcode();
    err_info = rsp_m2c.errmsg();
    return false;
  }
  // update policy
  if (rsp_m2c.has_notallocated() || !rsp_m2c.notallocated()) {
    sub_info_.CompAndSetNotAllocated(true, false);
  }
  if (rsp_m2c.has_defflowcheckid() || rsp_m2c.has_groupflowcheckid()) {
    if (rsp_m2c.has_defflowcheckid()) {
      rmtdata_cache_.UpdateDefFlowCtrlInfo(rsp_m2c.defflowcheckid(), rsp_m2c.defflowcontrolinfo());
    }
    int qryPriorityId = rsp_m2c.has_qrypriorityid() ? rsp_m2c.qrypriorityid()
                                                    : rmtdata_cache_.GetGroupQryPriorityId();
    rmtdata_cache_.UpdateGroupFlowCtrlInfo(qryPriorityId, rsp_m2c.groupflowcheckid(),
                                           rsp_m2c.groupflowcontrolinfo());
  }
  if (rsp_m2c.has_authorizedinfo()) {
    processAuthorizedToken(rsp_m2c.authorizedinfo());
  }
  error_code = err_code::kErrSuccess;
  err_info = "Ok";
  return true;
}

bool TubeMQConsumer::processHBResponseM2C(int32_t& error_code, string& err_info,
                                          const TubeMQCodec::RspProtocolPtr& rsp_protocol) {
  if (!rsp_protocol->success_) {
    error_code = rsp_protocol->code_;
    err_info = rsp_protocol->error_msg_;
    return false;
  }
  HeartResponseM2C rsp_m2c;
  bool result = rsp_m2c.ParseFromArray(rsp_protocol->rsp_body_.data().c_str(),
                                       (int32_t)(rsp_protocol->rsp_body_.data().length()));
  if (!result) {
    error_code = err_code::kErrParseFailure;
    err_info = "Parse HeartResponseM2C response failure!";
    return false;
  }
  if (!rsp_m2c.success()) {
    error_code = rsp_m2c.errcode();
    err_info = rsp_m2c.errmsg();
    return false;
  }
  // update policy
  if (rsp_m2c.has_notallocated() || !rsp_m2c.notallocated()) {
    sub_info_.CompAndSetNotAllocated(true, false);
  }
  if (rsp_m2c.has_defflowcheckid() || rsp_m2c.has_groupflowcheckid()) {
    if (rsp_m2c.has_defflowcheckid()) {
      rmtdata_cache_.UpdateDefFlowCtrlInfo(rsp_m2c.defflowcheckid(), rsp_m2c.defflowcontrolinfo());
    }
    int qryPriorityId = rsp_m2c.has_qrypriorityid() ? rsp_m2c.qrypriorityid()
                                                    : rmtdata_cache_.GetGroupQryPriorityId();
    rmtdata_cache_.UpdateGroupFlowCtrlInfo(qryPriorityId, rsp_m2c.groupflowcheckid(),
                                           rsp_m2c.groupflowcontrolinfo());
  }
  if (rsp_m2c.has_authorizedinfo()) {
    processAuthorizedToken(rsp_m2c.authorizedinfo());
  }
  if (rsp_m2c.has_requireauth()) {
    nextauth_2_master.Set(rsp_m2c.requireauth());
  }
  // Get the latest rebalance task
  if (rsp_m2c.has_event()) {
    EventProto eventProto = rsp_m2c.event();
    if (eventProto.rebalanceid() > 0) {
      list<SubscribeInfo> subcribe_infos;
      for (int i = 0; i < eventProto.subscribeinfo_size(); i++) {
        SubscribeInfo sub_info(eventProto.subscribeinfo(i));
        subcribe_infos.push_back(sub_info);
      }
      ConsumerEvent new_event(eventProto.rebalanceid(), eventProto.optype(), subcribe_infos, 0);
      rmtdata_cache_.OfferEvent(new_event);
    }
  }
  last_master_hbtime_ = Utils::GetCurrentTimeMillis();
  error_code = err_code::kErrSuccess;
  err_info = "Ok";
  return true;
}

void TubeMQConsumer::unregister2Brokers(map<NodeInfo, list<PartitionExt> >& unreg_partitions,
                                        bool wait_rsp) {
  string err_info;
  map<NodeInfo, list<PartitionExt> >::iterator it;
  list<PartitionExt>::iterator it_part;

  if (unreg_partitions.empty()) {
    return;
  }
  for (it = unreg_partitions.begin(); it != unreg_partitions.end(); ++it) {
    list<PartitionExt> part_list = it->second;
    for (it_part = part_list.begin(); it_part != part_list.end(); ++it_part) {
      auto request = std::make_shared<RequestContext>();
      TubeMQCodec::ReqProtocolPtr req_protocol = TubeMQCodec::GetReqProtocol();
      // build unregister 2 broker request
      buidUnRegRequestC2B(*it_part, it_part->IsLastConsumed(), req_protocol);
      request->codec_ = std::make_shared<TubeMQCodec>();
      request->ip_ = it_part->GetBrokerHost();
      request->port_ = it_part->GetBrokerPort();
      request->timeout_ = config_.GetRpcReadTimeoutMs();
      request->request_id_ = Singleton<UniqueSeqId>::Instance().Next();
      req_protocol->request_id_ = request->request_id_;
      req_protocol->rpc_read_timeout_ = config_.GetRpcReadTimeoutMs() - 500;
      // send message to target
      ResponseContext response_context;
      ErrorCode error = SyncRequest(response_context, request, req_protocol);
      if (wait_rsp) {
      }
      // not care result
      // TODO:  process result
    }
  }
}

bool TubeMQConsumer::processRegResponseB2C(int32_t& error_code, string& err_info,
                                           const TubeMQCodec::RspProtocolPtr& rsp_protocol) {
  if (!rsp_protocol->success_) {
    error_code = rsp_protocol->code_;
    err_info = rsp_protocol->error_msg_;
    return false;
  }
  RegisterResponseB2C rsp_b2c;
  bool result = rsp_b2c.ParseFromArray(rsp_protocol->rsp_body_.data().c_str(),
                                       (int)(rsp_protocol->rsp_body_.data().length()));
  if (!result) {
    error_code = err_code::kErrParseFailure;
    err_info = "Parse RegisterResponseB2C response failure!";
    return false;
  }
  if (!rsp_b2c.success()) {
    error_code = rsp_b2c.errcode();
    err_info = rsp_b2c.errmsg();
    return false;
  }
  error_code = err_code::kErrSuccess;
  err_info = "Ok";
  return true;
}

void TubeMQConsumer::convertMessages(int32_t& msg_size, list<Message>& message_list,
                                     bool filter_consume, const string& topic_name,
                                     GetMessageResponseB2C& rsp_b2c) {
  msg_size = 0;
  message_list.clear();
  if (rsp_b2c.messages_size() == 0) {
    return;
  }
  for (int i = 0; i < rsp_b2c.messages_size(); i++) {
    TransferedMessage tsfMsg = rsp_b2c.messages(i);
    int32_t flag = tsfMsg.flag();
    int64_t message_id = tsfMsg.messageid();
    int64_t in_check_sum = tsfMsg.checksum();
    int32_t payload_length = tsfMsg.payloaddata().length();
    std::unique_ptr<char[]> payload_data(new char[payload_length]);
    memcpy(&payload_data[0], tsfMsg.payloaddata().c_str(), payload_length);
    int64_t calc_checksum = 0;
    // TODO: calc crc 32
    if (in_check_sum != calc_checksum) {
      continue;
    }
    int read_pos = 0;
    int data_len = payload_length;
    map<string, string> properties;
    if ((flag & tb_config::kMsgFlagIncProperties) == 1) {
      if (payload_length < 4) {
        continue;
      }
      int32_t attr_len = ntohl(*(int*)(&payload_data[0]));
      read_pos += 4;
      data_len -= 4;
      if (attr_len > data_len) {
        continue;
      }
      string attribute(&payload_data[0] + read_pos, attr_len);
      read_pos += attr_len;
      data_len -= attr_len;
      Utils::Split(attribute, properties, delimiter::kDelimiterComma, delimiter::kDelimiterEqual);
      if (filter_consume) {
        map<string, set<string> > topic_filter_map = sub_info_.GetTopicFilterMap();
        map<string, set<string> >::const_iterator it = topic_filter_map.find(topic_name);
        if (properties.find(tb_config::kRsvPropKeyFilterItem) != properties.end()) {
          string msg_key = properties[tb_config::kRsvPropKeyFilterItem];
          if (it != topic_filter_map.end()) {
            set<string> filters = it->second;
            if (filters.find(msg_key) == filters.end()) {
              continue;
            }
          }
        }
      }
    }
    Message message(topic_name, flag, message_id, &payload_data[0] + read_pos, data_len,
                    properties);
    message_list.push_back(message);
    msg_size += data_len;
  }
  return;
}

bool TubeMQConsumer::processGetMessageRspB2C(ConsumerResult& result, PeerInfo& peer_info,
                                             bool filter_consume, const PartitionExt& partition_ext,
                                             const string& confirm_context,
                                             const TubeMQCodec::RspProtocolPtr& rsp) {
  string err_info;
  if (!rsp->success_) {
    rmtdata_cache_.RelPartition(err_info, filter_consume, confirm_context, false);
    result.SetFailureResult(rsp->code_, rsp->error_msg_, partition_ext.GetTopic(), peer_info);
    return false;
  }
  GetMessageResponseB2C rsp_b2c;
  bool ret_result =
      rsp_b2c.ParseFromArray(rsp->rsp_body_.data().c_str(), (int)(rsp->rsp_body_.data().length()));
  if (!ret_result) {
    rmtdata_cache_.RelPartition(err_info, filter_consume, confirm_context, false);
    result.SetFailureResult(err_code::kErrServerError,
                            "Parse GetMessageResponseB2C response failure!",
                            partition_ext.GetTopic(), peer_info);
    return false;
  }
  switch (rsp_b2c.errcode()) {
    case err_code::kErrSuccess: {
      bool esc_limit = (rsp_b2c.has_escflowctrl() && rsp_b2c.escflowctrl());
      long data_dltval =
          rsp_b2c.has_currdatadlt() ? rsp_b2c.currdatadlt() : tb_config::kInvalidValue;
      long curr_offset = rsp_b2c.has_curroffset() ? rsp_b2c.curroffset() : tb_config::kInvalidValue;
      bool req_slow = rsp_b2c.has_requireslow() ? rsp_b2c.requireslow() : false;
      int msg_size = 0;
      list<Message> message_list;
      convertMessages(msg_size, message_list, filter_consume, partition_ext.GetTopic(), rsp_b2c);
      rmtdata_cache_.BookedPartionInfo(partition_ext.GetPartitionKey(), curr_offset,
                                       err_code::kErrSuccess, esc_limit, msg_size, 0, data_dltval,
                                       req_slow);
      peer_info.SetCurrOffset(curr_offset);
      result.SetSuccessResult(err_code::kErrSuccess, partition_ext.GetTopic(), peer_info,
                              confirm_context, message_list);
      return true;
    }

    case err_code::kErrHbNoNode:
    case err_code::kErrCertificateFailure:
    case err_code::kErrDuplicatePartition: {
      rmtdata_cache_.RemovePartition(err_info, confirm_context);
      result.SetFailureResult(rsp_b2c.errcode(), rsp_b2c.errmsg(), partition_ext.GetTopic(),
                              peer_info);
      return false;
    }

    case err_code::kErrConsumeSpeedLimit: {
      // Process with server side speed limit
      long def_dlttime = rsp_b2c.has_minlimittime() ? rsp_b2c.minlimittime()
                                                    : config_.GetMsgNotFoundWaitPeriodMs();
      rmtdata_cache_.RelPartition(err_info, filter_consume, confirm_context, false,
                                  tb_config::kInvalidValue, rsp_b2c.errcode(), false, 0,
                                  def_dlttime, tb_config::kInvalidValue);
      result.SetFailureResult(rsp_b2c.errcode(), rsp_b2c.errmsg(), partition_ext.GetTopic(),
                              peer_info);
      return false;
    }

    case err_code::kErrNotFound:
    case err_code::kErrForbidden:
    case err_code::kErrMoved:
    case err_code::kErrServiceUnavilable:
    default: {
      // Slow down the request based on the limitation configuration when meet these errors
      long limit_dlt = 300;
      switch (rsp_b2c.errcode()) {
        case err_code::kErrForbidden: {
          limit_dlt = 2000;
          break;
        }
        case err_code::kErrServiceUnavilable: {
          limit_dlt = 300;
          break;
        }
        case err_code::kErrMoved: {
          limit_dlt = 200;
          break;
        }
        case err_code::kErrNotFound: {
          limit_dlt = config_.GetMsgNotFoundWaitPeriodMs();
          break;
        }
        default: {
          //
        }
      }
      rmtdata_cache_.RelPartition(err_info, filter_consume, confirm_context, false,
                                  tb_config::kInvalidValue, rsp_b2c.errcode(), false, 0, limit_dlt,
                                  tb_config::kInvalidValue);
      result.SetFailureResult(rsp_b2c.errcode(), rsp_b2c.errmsg(), partition_ext.GetTopic(),
                              peer_info);
      return false;
    }
  }
  return true;
}

bool TubeMQConsumer::isClientRunning() { return (this->status_.Get() == 2); }

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
  printf("getNextMasterAddr address is %s:%d", ipaddr.c_str(), port);
}

void TubeMQConsumer::getCurrentMasterAddr(string& ipaddr, int32_t& port) {
  ipaddr = curr_master_addr_;
  port = masters_map_[curr_master_addr_];
  if (Utils::NeedDnsXfs(ipaddr)) {
    TubeMQService::Instance()->GetXfsMasterAddress(curr_master_addr_, ipaddr);
  }
  printf("getCurrentMasterAddr address is %s:%d", ipaddr.c_str(), port);
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

void TubeMQConsumer::processAuthorizedToken(const MasterAuthorizedInfo& authorized_token_info) {
  visit_token_.Set(authorized_token_info.visitauthorizedtoken());
  if (authorized_token_info.has_authauthorizedtoken()) {
    lock_guard<mutex> lck(auth_lock_);

    if (authorized_info_ != authorized_token_info.authauthorizedtoken()) {
      authorized_info_ = authorized_token_info.authauthorizedtoken();
    }
  }
}

}  // namespace tubemq

