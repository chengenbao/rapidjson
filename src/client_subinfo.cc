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

#include "tubemq/client_subinfo.h"

#include <ctype.h>

#include "tubemq/const_config.h"
#include "tubemq/utils.h"



namespace tubemq {

using std::lock_guard;

MasterAddrInfo::MasterAddrInfo() {
  curr_addr_ = "";
  needXfs_ = false;
  master_source_.clear();
  master_target_.clear();
}

bool MasterAddrInfo::InitMasterAddress(string& err_info,
                                       const string& master_info) {
  master_source_.clear();
  Utils::Split(master_info, master_source_,
    delimiter::kDelimiterComma, delimiter::kDelimiterColon);
  if (master_source_.empty()) {
    err_info = "Illegal parameter: master_info is blank!";
    return false;
  }
  map<string, int32_t>::iterator it;
  for (it =master_source_.begin(); it != master_source_.end(); it++ ) {
    int8_t first_char =  it->first.c_str()[0];
    if (isalpha(first_char)) {
      needXfs_ = true;
      break;
    }
  }
  if (needXfs_) {
    UpdMasterAddrByDns();
  }
  it = master_source_.begin();
  curr_addr_ = it->first;
  err_info = "Ok";
  return true;
}

void MasterAddrInfo::GetNextMasterAddr(string& ipaddr, int32_t& port) {
  map<string, int32_t>::iterator it;
  it = master_source_.find(curr_addr_);
  if (it != master_source_.end()) {
    it++;
    if (it == master_source_.end()) {
      it = master_source_.begin();
    }
  } else {
    it = master_source_.begin();
  }
  ipaddr = it->first;
  port   = it->second;
  curr_addr_ = it->first;
  if (needXfs_) {
    lock_guard<mutex> lck(mutex_);
    ipaddr = master_target_[it->first];
  }
}

void MasterAddrInfo::GetCurrentMasterAddr(string& ipaddr, int32_t& port) {
  ipaddr = curr_addr_;
  port = master_source_[curr_addr_];
  if (needXfs_) {
    lock_guard<mutex> lck(mutex_);
    ipaddr = master_target_[curr_addr_];
  }
}

void MasterAddrInfo::UpdMasterAddrByDns() {
  map<string, string> tmp_addr_map;
  map<string, int32_t>::iterator it;
  if (!needXfs_ || master_source_.empty()) {
    return;
  }
  Utils::XfsAddrByDns(master_source_, tmp_addr_map);
  lock_guard<mutex> lck(mutex_);
  if (tmp_addr_map.empty()) {
    for (it =master_source_.begin(); it != master_source_.end(); it++ ) {
      this->master_target_[it->first] = it->first;
    }
  } else {
    this->master_target_ = tmp_addr_map;
  }
}




ClientSubInfo::ClientSubInfo() {
  bound_consume_ = false;
  select_big_ = false;
  source_count_ = 0;
  session_key_ = "";
  not_allocated_.Set(true);
  first_registered_.Set(false);
  subscribed_time_ = tb_config::kInvalidValue;
  bound_partions_ = "";
}


void ClientSubInfo::SetConsumeTarget(const ConsumerConfig& config) {
  int32_t count = 0;
  string tmpstr = "";
  // book register time
  subscribed_time_ = Utils::GetCurrentTimeMillis();
  //
  first_registered_.Set(false);
  bound_consume_ = config.IsBoundConsume();
  topic_and_filter_map_ = config.GetSubTopicAndFilterMap();
  // build topic filter info
  topics_.clear();
  topic_conds_.clear();
  set<string>::iterator it_set;
  map<string, set<string> >::const_iterator it_topic;
  for (it_topic = topic_and_filter_map_.begin();
      it_topic != topic_and_filter_map_.end(); it_topic++) {
    topics_.push_back(it_topic->first);
    if (it_topic->second.empty()) {
      topic_filter_map_[it_topic->first] = false;
    } else {
      topic_filter_map_[it_topic->first] = true;

      // build topic conditions
      count = 0;
      tmpstr = it_topic->first;
      tmpstr += delimiter::kDelimiterPound;
      for (it_set = it_topic->second.begin();
          it_set != it_topic->second.end(); it_set++) {
        if (count++ > 0) {
          tmpstr += delimiter::kDelimiterComma;
        }
        tmpstr += *it_set;
      }
      topic_conds_.push_back(tmpstr);
    }
  }

  // build bound_partition info
  if (bound_consume_) {
    session_key_ = config.GetSessionKey();
    source_count_ = config.GetSourceCount();
    select_big_ = config.IsSelectBig();
    assigned_part_map_ = config.GetPartOffsetInfo();
    count = 0;
    bound_partions_ = "";
    map<string, int64_t>::const_iterator it;
    for (it = assigned_part_map_.begin();
      it != assigned_part_map_.end(); it++) {
      if (count++ > 0) {
        bound_partions_ += delimiter::kDelimiterComma;
      }
      bound_partions_ += it->first;
      bound_partions_ += delimiter::kDelimiterEqual;
      bound_partions_ += Utils::Long2str(it->second);
    }
  }
}

bool ClientSubInfo::CompAndSetNotAllocated(bool expect, bool update) {
  return not_allocated_.CompareAndSet(expect, update);
}

bool ClientSubInfo::IsFilterConsume(const string& topic) {
  map<string, bool>::iterator it;
  it = topic_filter_map_.find(topic);
  if (it == topic_filter_map_.end()) {
    return false;
  }
  return it->second;
}

void ClientSubInfo::GetAssignedPartOffset(const string& partition_key, int64_t& offset) {
  map<string, int64_t>::iterator it;
  offset = tb_config::kInvalidValue;
  if (first_registered_.Get() && bound_consume_ && not_allocated_.Get()) {
    it = assigned_part_map_.find(partition_key);
    if (it != assigned_part_map_.end()) {
      offset = it->second;
    }
  }
}

const map<string, set<string> >& ClientSubInfo::GetTopicFilterMap() const {
  return topic_and_filter_map_;
}


}  // namespace tubemq

