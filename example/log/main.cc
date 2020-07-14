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

#include <chrono>
#include <exception>
#include <iostream>
#include <string>
#include <thread>

#include "tubemq/atomic_def.h"
#include "tubemq/logger.h"

using namespace std;
using namespace tubemq;

AtomicInteger ati;

void log() {
  while (1) {
    LOG_ERROR("atomic:%d", ati.IncrementAndGet());
  }
}

int main() {
  ati.GetAndSet(1);
  std::thread t1(log);
  std::thread t2(log);
  std::thread t3(log);
  std::thread t4(log);
  t1.join();
  return 0;
}

