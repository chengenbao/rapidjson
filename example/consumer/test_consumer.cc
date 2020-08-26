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
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string>
#include <libgen.h>
#include <sys/time.h>
#include <set>




#include "tubemq/client_service.h"
#include "tubemq/tubemq_client.h"
#include "tubemq/tubemq_config.h"
#include "tubemq/tubemq_message.h"
#include "tubemq/tubemq_return.h"
#include "tubemq/logger.h"



using namespace std;
using namespace tubemq;


int main(int argc, char* argv[]) {
  bool result;
  string err_info;
  string conf_file = "../conf/client.conf";
  string group_name = "test_c_v8";
  string master_addr = "100.115.158.208:8066,100.115.158.208:8066";
  TubeMQConsumer consumer_1;
  
  set<string> topic_list;
  topic_list.insert("test_1");
  ConsumerConfig consumer_config;

  consumer_config.SetRpcReadTimeoutMs(20000);
  result = consumer_config.SetMasterAddrInfo(err_info, master_addr);
  if (!result) {
    printf("\n Set Master AddrInfo failure: %s \n", err_info.c_str());
    return -1;
  }
  result = consumer_config.SetGroupConsumeTarget(err_info, group_name, topic_list);
  if (!result) {
    printf("\n Set GroupConsume Target failure: %s\n",err_info.c_str());
    return -1;
  }
  result = StartTubeMQService(err_info, conf_file);
  if (!result) {
    printf("\n Initial Tube service failure \n");
    return -1;
  }

  result = consumer_1.Start(err_info, consumer_config);
  if (!result) {
    printf(" Initial consumer failure, error is: %s \n", err_info.c_str());
    return -2;
  }

  ConsumerResult gentRet;

  getchar();
  consumer_1.ShutDown();

  getchar();
  result = StopTubeMQService(err_info);
  if(!result) {
    printf(" *** stopTubeService failure, reason is %s \n", err_info.c_str());
  }

  getchar();
  printf("\n finishe test exist \n");
  return 0;
}




