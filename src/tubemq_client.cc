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

namespace tubemq {


TubeMQConsumer::TubeMQConsumer() : BaseClient(false) {
  status_.Set(0);
  client_uuid_ = "";
}

BaseConsumerClient::~TubeBaseConsumer() {

}

bool BaseConsumerClient::Start(string& err_info, const ConsumerConfig& config) {
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
  
  
	_clientLocId = TimeService::instance()->GetAndAddIndex();
	_clientStrId = genClientStrId();
    SetInterval(_clientConfig.getHeartbeatTimeout());
	if(TimeService::instance()->AddTaskAndRun(_clientLocId, this))
	{
    	this->_clientStatusId.set(READY);
		errInfo = "The client CB has existed!";
		return false;
	}
    if(!register2Master(errInfo,false)) 
    {
    	this->_clientStatusId.set(READY);
        return false;        
    }
    SetFlag(true);
    errInfo = "Ok";
    return true;  
}





}  // namespace tubemq

