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

#include "tubemq/client_service.h"
#include <sstream>


namespace tubemq {

using std::stringstream;


BaseClient::BaseClient(bool is_producer) {
  this->is_producer_ = is_producer;
}

BaseClient::~BaseClient() {
  // no code
}

bool TubeService::AddClientObj(string& err_info, 
           const BaseClient* client_obj, int32_t& client_index) {
  if (service_status_.Get() != 0) {
    err_info = "Service not startted!";
    return false;
  }
  pthread_rwlock_wrlock(&mutex_);
  client_index = client_index_base_.IncrementAndGet();
  this->clients_map_[client_index] = client_obj;
  pthread_rwlock_unlock(&mutex_);
  err_info = "Ok";
  return true;
}

BaseClient* TubeService::GetClientObj(int32_t client_index) {

}



}  // namespace tubemq
