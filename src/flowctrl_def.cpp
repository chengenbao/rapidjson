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

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sstream>
#include "const_config.h"
#include "flowctrl_def.h"






namespace tubemq {


FlowCtrlResult::FlowCtrlResult() {
  this->datasize_limit_ = config::kMaxIntValue;  
  this->freqms_limit_ = 0;
}

FlowCtrlResult::FlowCtrlResult(long datasize_limit, int freqms_limit) {
  this->datasize_limit_ = datasize_limit;  
  this->freqms_limit_ = freqms_limit;
}

FlowCtrlResult& FlowCtrlResult::operator=(const FlowCtrlResult& target) {
  if (this == &target) 
    return *this;
  this->datasize_limit_ = target.datasize_limit_;  
  this->freqms_limit_ = target.freqms_limit_;
  return *this;
}

void FlowCtrlResult::SetDataDltAndFreqLimit(long datasize_limit, int freqms_limit) {
  this->datasize_limit_ = datasize_limit;
  this->freqms_limit_ = freqms_limit;
}

void FlowCtrlResult::SetDataSizeLimit(long datasize_limit) {
  this->datasize_limit_ = datasize_limit;
}

void FlowCtrlResult::SetFreqMsLimit(int freqms_limit) {
  this->freqms_limit_ = freqms_limit;
}

long FlowCtrlResult::GetDataSizeLimit() {
  return this->datasize_limit_;
}

int FlowCtrlResult::GetFreqMsLimit() {
  return this->freqms_limit_;
}


FlowCtrlItem::FlowCtrlItem() {
  this->type_           = 0;
  this->start_time_     = 2500;
  this->end_time_       = config::kInvalidValue;
  this->datadlt_m_      = config::kInvalidValue;
  this->datasize_limit_ = config::kInvalidValue;
  this->freqms_limit_   = config::kInvalidValue;
  this->zero_cnt_       = config::kInvalidValue;
}

FlowCtrlItem::FlowCtrlItem(int type,int zero_cnt,int freqms_limit) {
  this->type_           = type;
  this->start_time_     = 2500;
  this->end_time_       = config::kInvalidValue;
  this->datadlt_m_      = config::kInvalidValue;
  this->datasize_limit_ = config::kInvalidValue;
  this->freqms_limit_   = freqms_limit;
  this->zero_cnt_       = zero_cnt;
}

FlowCtrlItem::FlowCtrlItem(int type, 
  int datasize_limit,int freqms_limit,int min_data_filter_freqms) {
  this->type_           = type;
  this->start_time_     = 2500;
  this->end_time_       = config::kInvalidValue;
  this->datadlt_m_      = config::kInvalidValue;
  this->datasize_limit_ = datasize_limit;
  this->freqms_limit_   = freqms_limit;
  this->zero_cnt_       = min_data_filter_freqms;
}

FlowCtrlItem::FlowCtrlItem(int type, int start_time, int end_time, 
  long datadlt_m, long datasize_limit, int freqms_limit) {
  this->type_           = type;
  this->start_time_     = start_time;
  this->end_time_       = end_time;
  this->datadlt_m_      = datadlt_m;
  this->datasize_limit_ = datasize_limit;
  this->freqms_limit_   = freqms_limit;
  this->zero_cnt_       = config::kInvalidValue;
}

FlowCtrlItem& FlowCtrlItem::operator=(const FlowCtrlItem& target) {
  if (this == &target) 
    return *this;
  this->type_           = target.type_;
  this->start_time_     = target.start_time_;
  this->end_time_       = target.end_time_;
  this->datadlt_m_      = target._dltInM;
  this->datasize_limit_ = target.datasize_limit_;
  this->freqms_limit_   = target.freqms_limit_;
  this->zero_cnt_       = target.zero_cnt_;
    return *this;
}

int FlowCtrlItem::GetFreLimit(int msg_zero_cnt) {
  if (this->type_ != 1) {
    return -1;
  }
  if (msg_zero_cnt >= this->zero_cnt_) {
    return this->freqms_limit_;
  }
  return -1;
}

void FlowCtrlItem::ResetFlowCtrlValue(int type, 
  int datasize_limit,int freqms_limit,int min_data_filter_freqms) {
  this->type_           = type;
  this->start_time_     = 2500;
  this->end_time_       = config::kInvalidValue;
  this->datadlt_m_      = config::kInvalidValue;
  this->datasize_limit_ = datasize_limit;
  this->freqms_limit_   = freqms_limit;
  this->zero_cnt_       = min_data_filter_freqms;
}

void FlowCtrlItem::Clear() {
  this->type_           = 0;
  this->start_time_     = 2500;
  this->end_time_       = config::kInvalidValue;
  this->datadlt_m_      = config::kInvalidValue;
  this->datasize_limit_ = config::kInvalidValue;
  this->freqms_limit_   = config::kInvalidValue;
  this->zero_cnt_       = config::kInvalidValue;
}

bool FlowCtrlItem::GetDataLimit(long datadlt_m, int curr_time, FlowCtrlResult& flowctrl_result) {
  if (this->type_ != 0 || datadlt_m <= this->datadlt_m_ ) {
    return false;
  }
  if (curr_time < this->start_time_ 
    || curr_time > this->this->end_time_) {
    return false;
  }
  flowctrl_result.SetDataDltAndFreqLimit(this->datasize_limit_,this->freqms_limit_);
  return true;
}


FlowCtrlRuleHandler::FlowCtrlRuleHandler() {
  this->flowctrl_id_.getAndSet(config::kInvalidValue);
  this->flowctrl_info_ = "";
  this->min_zero_cnt_.Set(config::kMaxIntValue);
  this->qrypriority_id_.Set(config::kInvalidValue);
  this->min_datadlt_limt_.Set(config::kMaxLongValue);
  this->datalimit_start_time_.Set(2500);
  this->datalimit_end_time_.Set(config::kInvalidValue);
  this->last_update_time_ = Utils::GetCurrentTimeMillis();
  pthread_rwlock_init(&configrw_lock_, NULL);  
}

FlowCtrlRuleHandler::~FlowCtrlRuleHandler() {
  pthread_rwlock_destroy(&configrw_lock_);
}

void FlowCtrlRuleHandler::UpdateDefFlowCtrlInfo(bool is_default, 
  int qrypriority_id, long flowctrl_id, const string& flowctrl_info) {
  bool result;
  map<int, vector<FlowCtrlItem> > tmp_flowctrl_map;
  if (flowctrl_id == this->flowctrl_id_.get()) {
      return;
  }
  long curr_flowctrl_id = this->flowctrl_id_.get();
  if (flowctrl_info.length > 0) {
    result = parseFlowCtrlInfo(flowctrl_info, tmp_flowctrl_map);
  }
  pthread_rwlock_wrlock(&this->configrw_lock_);
  this->flowctrl_id_.set(flowctrl_id);
  this->qrypriority_id_.set(qrypriority_id);
  clearStatisData();
  if (tmp_flowctrl_map.empty()) {
    this->flowctrl_rules_.clear();
    this->flowctrl_info_ = "";
  } else {
    this->flowctrl_rules_ = tmp_flowctrl_map;
    this->flowctrl_info_ = flowctrl_info;
    initialStatisData();
  }
  this->last_update_time_ = Utils::GetCurrentTimeMillis();
  pthread_rwlock_unlock(&this->configrw_lock_);
  if (is_default) {
    LogRText(INFO,"[Flow Ctrl] Default FlowCtrl Rule flowctrl_id from %ld updated to %ld\n",curr_flowctrl_id,flowctrl_id);
  } else {
    LogRText(INFO,"[Flow Ctrl] Group FlowCtrl Rule flowctrl_id from %ld updated to %ld\n",curr_flowctrl_id,flowctrl_id);
  }
  return;

}


void FlowCtrlRuleHandler::initialStatisData() {
  vector<FlowCtrlItem>::iterator it_vec;
  map<int, vector<FlowCtrlItem> >::iterator it_map;

  it_map = this->flowctrl_rules_.find(0);
  if (it_map != this->flowctrl_rules_.end()) {
    for (it_vec = it_map->second.begin(); it_vec != it_map->second.end(); ++it_vec) {
      if (it_vec->GetType() != 0) {
        continue;
      }

      if (it_vec->GetDltInM() < this->min_datadlt_limt_.get()) {
        this->min_datadlt_limt_.set(it_vec->GetDltInM());
      }
      if (it_vec->GetStartTime() < this->datalimit_start_time_.get()) {
        this->datalimit_start_time_.Set(it_vec->GetStartTime());
      }
      if (it_vec->GetEndTime() > this->datalimit_end_time_.get()) {
        this->datalimit_end_time_.Set(it_vec->GetEndTime());
      }
    }
  }
  it_map = this->flowctrl_rules_.find(1);
  if (it_map != this->flowctrl_rules_.end()) {
    for (it_vec = it_map->second.begin(); it_vec != it_map->second.end(); ++it_vec) {
      if (it_vec->GetType() != 1) {
          continue;
      }
      if (it_vec->GetZeroCnt() < this->min_zero_cnt_.get()) {
        this->min_zero_cnt_.set(it_vec->GetZeroCnt());
      }
    }
  }
  it_map = this->flowctrl_rules_.find(3);
  if (it_map != this->flowctrl_rules_.end()) {
    for (it_vec = it_map->second.begin(); it_vec != it_map->second.end(); ++it_vec) {
      if (it_vec->GetType() != 3) {
        continue;
      }
      it_vec->GetDataSizeLimit()
      this->filter_ctrl_item_.ResetFlowCtrlValue(3,
          (int)(it_vec->GetDataSizeLimit()),it_vec->GetFreqMsLimit(),it_vec->GetZeroCnt());
    }
  }
}

void FlowCtrlRuleHandler::clearStatisData() {
  this->min_zero_cnt_.getAndSet(config::kMaxIntValue);
  this->min_datadlt_limt_.getAndSet(config::kMaxLongValue);
  this->qrypriority_id_.set(config::kInvalidValue);
  this->datalimit_start_time_.Set(2500);
  this->datalimit_end_time_.Set(config::kInvalidValue);
  this->filter_ctrl_item_.clear();
}

bool FlowCtrlRuleHandler::GetCurDataLimit(long last_datadlt, FlowCtrlResult& flowctrl_result) {
  struct tm utc_tm;
  vector<FlowCtrlItem>::iterator it_vec;
  map<int, vector<FlowCtrlItem> >::iterator it_map;
  time_t cur_time=time(NULL);

  gmtime_r(&cur_time,&utc_tm);
  int curr_time = (utc_tm.tm_hour+8)%24 * 100 + utc_tm.tm_min;
  if ((last_datadlt < this->min_datadlt_limt_.get()) 
    || (curr_time < this->datalimit_start_time_.Get())
    || (curr_time > this->datalimit_end_time_.Get())) {
    return false;
  }
  it_map = this->flowctrl_rules_.find(0);
  if (it_map == this->flowctrl_rules_.end()) {
    return false;
  }
  for (it_vec = it_map->second.begin(); it_vec != it_map->second.end(); ++it_vec) {
    if (it_vec->getDataLimit(last_datadlt,curr_time,flowctrl_result)) {
      return true;
    }
  }
  return false;
}

int FlowCtrlRuleHandler::GetCurFreqLimitTime(int msg_zero_cnt, int received_limit)
{
  int rule_val = -2;
  vector<FlowCtrlItem>::iterator it_vec;
  map<int, vector<FlowCtrlItem> >::iterator it_map;

  if (msg_zero_cnt < this->min_zero_cnt_.get()) {
    return received_limit;
  }
  it_map = this->flowctrl_rules_.find(1);
  if (it_map == this->flowctrl_rules_.end()) {
    return received_limit;
  }
  for (it_vec = it_map->second.begin(); it_vec != it_map->second.end(); ++it_vec) {
    rule_val = it_vec->GetFreLimit(msg_zero_cnt);
    if (rule_val >= 0) 
    {
      return rule_val;
    }
  }
  return received_limit;
}


bool FlowCtrlRuleHandler::compareDataLimitQueue(const FlowCtrlItem& o1, const FlowCtrlItem& o2) {
  if (o1.GetStartTime() >= o2.GetStartTime()) {
    return true;
  }
  return false;
}

bool FlowCtrlRuleHandler::compareFeqQueue(const FlowCtrlItem& queue1, const FlowCtrlItem& queue2) {
    return (queue1.getZeroCnt() < queue2.getZeroCnt());
}

bool FlowCtrlRuleHandler::parseFlowCtrlInfo(const string& flowctrl_info, 
                                map<int,vector<FlowCtrlItem> >& flowctrl_info_map) {
  int type;
  string err_info;
  stringstream ss;
  rapidjson::Document doc;
  // check flowctrl info length
  if (flowctrl_info.length() == 0){
    return false;
  }
  // parse flowctrl info
  if(doc.Parse(flowctrl_info).HasParseError()) {
    LogRText(ERROR,"arsing error, flowCtrlInfo's value must be valid json format! flowctrl_info=%s\n",flowctrl_info.c_str()); 
    return false;
  }
  if (!doc.IsArray()) {
    LogRText(ERROR,"flowCtrlInfo's value must be dict! flowctrl_info=%s\n",flowctrl_info.c_str()); 
    return false;
  }
  for (unsigned int i = 0; i < doc.Size(); i++) {
    vector<FlowCtrlItem> flowctrl_item_vec;
    const rapidjson::Value& node_item = doc[i];
    if(!node_item.IsObject()) {
      continue;
    }
    if (!parseIntMember(err_info, node_item, "type", type, false, -2)) {
      ss << "Decode Failure: ";
      ss << err_info;
      ss << " of type field in parse flowctrl_info!";
      err_info = ss.str();
      LogRText(ERROR,"parseFlowCtrlInfo failure %s",err_info.c_str()); 
      return false;
    }    
    if (type < 0 || type > 3) {
      ss << "type value must in [0,1,2,3] in index(";
      ss << i;
      ss << ") of flowctrl_info value!";
      err_info = ss.str();
      LogRText(ERROR,"parseFlowCtrlInfo failure %s",err_info.c_str()); 
      return false;
    }
    
    switch (type) {
      case 1: {
        if (FlowCtrlRuleHandler::parseFreqLimit(err_info, node_item, flowctrl_item_vec)) {
          flowctrl_info_map[1]=flowctrl_item_vec;
        } else {
          LogRText(ERROR,"parseFlowCtrlInfo parseFreqLimit failure: %s",err_info.c_str()); 
        }
      }
      break;
       
      case 3: {
        if (FlowCtrlRuleHandler::parseLowFetchLimit(err_info, node_item, flowctrl_item_vec)) {
            flowctrl_info_map[3]=flowctrl_item_vec;
        } else {
          LogRText(ERROR,"parseFlowCtrlInfo parseLowFetchLimit failure: %s",err_info.c_str()); 
        }
      }
      break;

      case 0: {
        if (FlowCtrlRuleHandler::parseDataLimit(err_info, node_item, flowctrl_item_vec)) {
          flowctrl_info_map[0]=flowctrl_item_vec;
        } else {
          LogRText(ERROR,"parseFlowCtrlInfo parseDataLimit failure: %s",err_info.c_str()); 
        }
      }
      break;

      default:
        break;
    }
  }  
  return true;
}

bool FlowCtrlRuleHandler::parseDataLimit(string& err_info, 
                           const rapidjson::Value& root, vector<FlowCtrlItem>& flowctrl_items) {
  int type_val;
  stringstream ss;
  string attr_sep = delimiter::kDelimiterColon;
  string::size_type pos1;
  if (!parseIntMember(err_info, root, "type", type_val, true, 0)) {
    ss << "Decode Failure: ";
    ss << err_info;
    ss << " of type field in parse data limit!";
    err_info = ss.str();
    return false;
  }
  // check rule type
  if (!root.HasMember("rule")) {
    err_info = "rule field not existed";
    return false;
  }
  if (!root["rule"].IsArray()) {
    err_info = "Illegal value, rule must be list type";
    return false;
  }
  // parse rule info
  const rapidjson::Value& obj_set = root["rule"];
  for (unsigned int index = 0 ; index < obj_set.Size() ; index++) {
    int start_time = 0;
    int end_time = 0;
    long datadlt_m = 0;
    long datasize_limit = 0;
    int freqms_limit = 0;
    const rapidjson::Value& node_item = obj_set[index];
    if (!node_item.IsObject()) {
        err_info = "Illegal rule'value item, must be dict type";
        return false;
    }
    if (!parseTimeMember(err_info, node_item, "start", start_time)) {
      return false;
    }
    if (!parseTimeMember(err_info, node_item, "end", end_time)) {
      return false;
    }
    if (start_time > end_time) {
      ss << "start value must lower than the End value in index(";
      ss << index;
      ss << ") of data limit rule!";
      err_info = ss.str();
      return false;
    }
    if (!parseLongMember(err_info, node_item, "dltInM", datadlt_m, false, -1)) {
      ss << "dltInM key is required in index(";
      ss << index;
      ss << ") of data limit rule!";
      err_info = ss.str();
      return false;
    }
    if (datadlt_m <= 20) {
      ss << "dltInM value must over than 20 in index(";
      ss << index;
      ss << ") of data limit rule!";
      err_info = ss.str();
      return false;
    }
    datadlt_m = datadlt_m * 1024 * 1024;
    if (!parseLongMember(err_info, node_item, "limitInM", datasize_limit, false, -1)) {
      ss << "limitInM key is required in index(";
      ss << index;
      ss << ") of data limit rule!";
      err_info = ss.str();
      return false;
    }
    if (datasize_limit < 0) {
      ss << "limitInM value must over than equal or bigger than zero in index(";
      ss << index;
      ss << ") of data limit rule!";
      err_info = ss.str();
      return false;
    }    
    datasize_limit = datasize_limit * 1024 * 1024;
    if (!parseIntMember(err_info, node_item, "freqInMs", freqms_limit, false, -1)) {
      ss << "freqInMs key is required in index(";
      ss << index;
      ss << ") of data limit rule!";
      err_info = ss.str();
      return false;
    }
    if (freqms_limit < 200) {
      ss << "freqInMs value must over than equal or bigger than 200 in index(";
      ss << index;
      ss << ") of data limit rule!";
      err_info = ss.str();
      return false;
    }    
    FlowCtrlItem flowctrl_item(0, start_time, end_time, datadlt_m, datasize_limit, freqms_limit);
    flowctrl_items.push_back(flowctrl_item);
  }
  if (!flowctrl_items.empty()) {
    std::sort(flowctrl_items.begin(), flowctrl_items.end(), compareDataLimitQueue);
  }
  err_info = "Ok";
  return true;  
}

bool FlowCtrlRuleHandler::parseFreqLimit(string& err_info, 
                           const rapidjson::Value& root, vector<FlowCtrlItem>& flowctrl_items) {
  int type_val;
  stringstream ss;

  if (!parseIntMember(err_info, root, "type", type_val, true, 1)) {
    ss << "Decode Failure: ";
    ss << err_info;
    ss << " of type field in parse freq limit!";
    err_info = ss.str();
    return false;
  }
  if (!root.HasMember("rule")) {
    err_info = "rule field not existed";
    return false;
  }
  if (!root["rule"].IsArray()) {
    err_info = "Illegal value, rule must be list type";
    return false;
  }
  // parse rule info
  const rapidjson::Value& obj_set = root["rule"];
  for (unsigned int i = 0 ; i < obj_set.Size() ; i++) {
    int zeroCnt = -2;
    int freqms_limit = -2;
    const rapidjson::Value& node_item = obj_set[i];
    if (!node_item.IsObject()) {
      err_info = "Illegal rule'value item, must be dict type";
      return false;
    }
    if (!parseIntMember(err_info, node_item, "zeroCnt", zeroCnt, false, -2)) {
      ss << "Decode Failure: ";
      ss << err_info;
      ss << " of zeroCnt field in parse freq limit!";
      err_info = ss.str();
      return false;
    }
    if (!parseIntMember(err_info, node_item, "freqInMs", freqms_limit, false, -2)) {
      ss << "Decode Failure: ";
      ss << err_info;
      ss << " of freqInMs field in parse freq limit!";
      err_info = ss.str();
      return false;
    }
    FlowCtrlItem flowctrl_item(1, zeroCnt, freqms_limit);
    flowctrl_items.push_back(flowctrl_item);
  }
  if (!flowctrl_items.empty()) {
    std::sort(flowctrl_items.begin(), flowctrl_items.end(), compareFeqQueue);
  }
  err_info = "Ok";
  return true;  
}

bool FlowCtrlRuleHandler::parseLowFetchLimit(string& err_info, 
                            const rapidjson::Value& root, vector<FlowCtrlItem>& flowctrl_items) {
  int type_val;
  stringstream ss;
  if (!parseIntMember(err_info, root, "type", type_val, true, 3)) {
    ss << "Decode Failure: ";
    ss << err_info;
    ss << " of type field in parse low fetch limit!";
    err_info = ss.str();
    return false;
  }
  if (!root.HasMember("rule")) {
    err_info = "rule field not existed";
    return false;
  }
  if (!root["rule"].IsArray()) {
    err_info = "Illegal value, rule must be list type";
    return false;
  }
  // parse rule info
  const rapidjson::Value& node_item = root["rule"];
  for (unsigned int i = 0 ; i < node_item.Size() ; i++) {
    int norm_freq_ms = 0;
    int filter_freq_ms = 0;
    int min_filter_freq_ms = 0;
    FlowCtrlItem flowctrl_item;
    const rapidjson::Value& node_item = node_item[i];
    if (!node_item.IsObject()) {
      err_info = "Illegal rule'value item, must be dict type";
      return false;
    }
    if (node_item.HasMember("filterFreqInMs") 
      || node_item.HasMember("minDataFilterFreqInMs")) {
      if (!parseIntMember(err_info, node_item, 
        "filterFreqInMs", filter_freq_ms, false, -1)) {
        ss << "Decode Failure: ";
        ss << err_info;
        ss << " of filterFreqInMs field in parse low fetch limit!";
        err_info = ss.str();
        return false;
      }
      if (!parseIntMember(err_info, node_item, 
        "minDataFilterFreqInMs", min_filter_freq_ms, false, -1)) {
        ss << "Decode Failure: ";
        ss << err_info;
        ss << " of minDataFilterFreqInMs field in parse low fetch limit!";
        err_info = ss.str();
        return false;
      }
      if (filter_freq_ms < 0 || filter_freq_ms > 300000) {
        ss << "Decode Failure: ";
        ss << "filterFreqInMs value must in [0, 300000] in index(";
        ss << i;
        ss << ") of low fetch limit rule!";
        err_info = ss.str();
        return false;
      }
      if (min_filter_freq_ms < 0 || min_filter_freq_ms > 300000) {
        ss << "Decode Failure: ";
        ss << "minDataFilterFreqInMs value must in [0, 300000] in index(";
        ss << i;
        ss << ") of low fetch limit rule!";
        err_info = ss.str();
        return false;
      }
      if (min_filter_freq_ms < filter_freq_ms) {
        ss << "Decode Failure: ";
        ss << "minDataFilterFreqInMs must lower than filterFreqInMs in index(";
        ss << i;
        ss << ") of low fetch limit rule!";
        err_info = ss.str();
        return false;
      }
    }
    if (node_item.HasMember("normFreqInMs")) {
      if (!parseIntMember(err_info, node_item, 
        "normFreqInMs", norm_freq_ms, false, -1)) {
        ss << "Decode Failure: ";
        ss << err_info;
        ss << " of normFreqInMs field in parse low fetch limit!";
        err_info = ss.str();
        return false;
      }
      if (norm_freq_ms < 0 || norm_freq_ms > 300000) {
        ss << "Decode Failure: ";
        ss << "normFreqInMs value must in [0, 300000] in index(";
        ss << i;
        ss << ") of low fetch limit rule!";
        err_info = ss.str();
        return false;
      }        
    }
    flowctrl_item.ResetFlowCtrlValue(3,norm_freq_ms,filter_freq_ms,min_filter_freq_ms);
    flowctrl_items.push_back(flowctrl_item);
  }
  err_info = "Ok";
  return true;
}

bool FlowCtrlRuleHandler::parseStringMember(string& err_info, const rapidjson::Value& root, 
                            const char* key, string& value, bool compare_value, string required_val) {
  // check key if exist
  if (!root.HasMember(key)) {
    err_info = "Field not existed";
    return false;
  }
  if (!root[key].IsString()) {
    err_info = "Illegal value, must be string type";
    return false;
  }
  
  if (compare_value) {
    if (root[key].GetString() != required_val) {
      err_info = "Illegal value, not required value content";
      return false;
    }
  }
  value = root[key].GetString();
  return true;
}

bool FlowCtrlRuleHandler::parseLongMember(string& err_info, const rapidjson::Value& root, 
                            const char* key, long& value, bool compare_value, long required_val) {
  if (!root.HasMember(key)) {
    err_info = "Field not existed";
    return false;
  }
  if (!root[key].IsNumber()) {
    err_info = "Illegal value, must be number type";
    return false;
  }
  if (compare_value) {
    if ((long)root[key].GetInt64() != required_val) {
      err_info = "Illegal value, not required value content";
      return false;
    }
  }
  value = (long)root[key].GetInt64();
  return true;
}

bool FlowCtrlRuleHandler::parseIntMember(string& err_info, const rapidjson::Value& root, 
                            const char* key, int& value, bool compare_value, int required_val) {
  if (!root.HasMember(key)) {
    err_info = "Field not existed";
    return false;
  }
  if (!root[key].IsInt()) {
    err_info = "Illegal value, must be int type";
    return false;
  }
  if (compare_value) {
    if (root[key].GetInt() != required_val) {
      err_info = "Illegal value, not required value content";
      return false;
    }
  }
  value = root[key].GetInt();
  return true;
}

bool FlowCtrlRuleHandler::parseTimeMember(string& err_info, 
                           const rapidjson::Value& root, const char* key, int& value) {
  // check key if exist
  stringstream ss;
  if (!root.HasMember(key)) {
    ss << "field ";
    ss << key;
    ss << " not existed!";
    err_info = ss.str();
    return false;
  }
  if (!root[key].IsString()) {
    ss << "field ";
    ss << key;
    ss << " must be string type!";
    err_info = ss.str();
    return false;
  }
  string::size_type pos1;
  string str_value = root[key].GetString();
  string attr_sep = delimiter::kDelimiterColon;
  pos1 = str_value.find(attr_sep);
  if (string::npos == pos1) {
    ss << "field ";
    ss << key;
    ss << " must be 'aa:bb' and 'aa','bb' must be int value format!";
    err_info = ss.str();
    return false;
  }
  string sub_str_1 = str_value.substr(0,pos1);
  string sub_str_2 = 
    str_value.substr(pos1+attr_sep.size(),str_value.size());
  int in_hour = atoi(sub_str_1.c_str());
  int in_minute = atoi(sub_str_2.c_str());
  if (in_hour < 0 || in_hour > 24) {
    ss << "field ";
    ss << key;
    ss << " -hour value must in [0,23]!";
    err_info = ss.str();
    return false;      
  }
  if (in_minute < 0 || in_minute > 59) {
    ss << "field ";
    ss << key;
    ss << " -minute value must in [0,59]!";
    err_info = ss.str();
    return false;      
  }
  value = in_hour * 100 + in_minute;
  return true;
}

 }


}



