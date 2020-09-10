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
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <string>
#include <libgen.h>
#include <sys/time.h>
#include <chrono>
#include <set>
#include <thread>
#include "tubemq/tubemq_client.h"
#include "tubemq/tubemq_config.h"
#include "tubemq/tubemq_errcode.h"
#include "tubemq/tubemq_message.h"
#include "tubemq/tubemq_return.h"
#include "tubemq/tubemq_errcode.h"


using namespace std;
using namespace tubemq;


int main(int argc, char* argv[]) {
  bool result;
  string err_info;
  string conf_file = "../conf/client.conf";
  string group_name = "test_c_v8";
  string master_addr = "10.215.128.83:8000,10.215.128.83:8000";
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
    printf("\n Set GroupConsume Target failure: %s\n", err_info.c_str());
    return -1;
  }
  result = StartTubeMQService(err_info, conf_file);
  if (!result) {
    printf("\n StartTubeMQService failure: %s \n", err_info.c_str());
    return -1;
  }

  result = consumer_1.Start(err_info, consumer_config);
  if (!result) {
    printf(" Initial consumer failure, error is: %s \n", err_info.c_str());
    return -2;
  }

  ConsumerResult gentRet;
  ConsumerResult confirm_result;
  int64_t start_time = time();
  do {
    // 1. get Message;
    result = consumer_1.GetMessage(gentRet);
    if (result) {
      // 2.1.1  if success, process message
      list<Message> msgs = gentRet.GetMessageList();
      printf(" GetMessage success, msssage count =%ld \n", msgs.size());
      // 2.1.2 confirm message result
      consumer_1.Confirm(gentRet.GetConfirmContext(), true, confirm_result);
    } else {
      // 2.2.1 if failure, check error code
      // if no partitions assigned, all partitions in use,
      //    or all partitons idle, sleep and retry
      if (gentRet.GetErrCode() == err_code::kErrNoPartAssigned
        || gentRet.GetErrCode() == err_code::kErrAllPartInUse
        || gentRet.GetErrCode() == err_code::kErrAllPartWaiting) {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
      } else {
        // 2.2.2 if another error, print error message
        printf(" GetMessage failure, err_code=%d, err_msg is: %s \n",
          gentRet.GetErrCode(), gentRet.GetErrMessage().c_str());
      }
    }
    // used for test, consume 10 minutes only
    if (time() - start_time > 60) {
      break;
    }
  } while (true);

  getchar();
  consumer_1.ShutDown();

  getchar();
  result = StopTubeMQService(err_info);
  if (!result) {
    printf(" *** StopTubeMQService failure, reason is %s \n", err_info.c_str());
  }

  getchar();
  printf("\n finishe test exist \n");
  return 0;
}




