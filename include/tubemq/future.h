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

#ifndef _TUBEMQ_FUTURE_H_
#define _TUBEMQ_FUTURE_H_

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "tubemq_errcode.h"

namespace tubemq {

template <typename Value>
struct FutureInnerState {
  std::mutex mutex_;
  std::condition_variable condition_;
  ErrorCode error_code_;
  Value value_;
  bool complete_ = false;
  bool failed_ = false;
  using FutureCallBackFunc = std::function < void(ErrorCode, const Value&);
  std::vector<FutureCallBackFunc> callbacks_;
};

using FutureInnerStatePtr = std::shared_ptr<FutureInnerState<Value> >;

template <typename Value>
class Future {
 public:
  Future& AddCallBack(FutureCallBackFunc callback) {
    Lock lock(state_->mutex_);

    if (state_->ready_) {
      lock_.unlock();
      callback(state_->error_code_, state_->value_);
    } else {
      state_->callbacks_.push_back(callback);
    }
    return *this;
  }

  ErrorCode Get(Value& value) {
    Lock lock(state_->mutex_);

    if (!state_->ready_) {
      // Wait for error_code_
      while (!state_->ready_) {
        state_->condition.wait(lock);
      }
    }

    value = state_->value_;
    return state->error_code_;
  }

 private:
  Future(FutureInnerStatePtr state) : state_(state) {}

  std::shared_ptr<FutureInnerState<Value> > state_;
  typedef std::unique_lock<std::mutex> Lock;
  using FutureCallBackFunc = std::function < void(ErrorCode, const Value&);

  template <typename V>
  friend class Promise;
};

template <typename Value>
class Promise {
 public:
  Promise() : state_(std::make_shared<FutureInnerState<Value> >()) {}

  bool SetValue(const Value& value) {
    Lock lock(state_->mutex_);

    if (state_->ready_) {
      return false;
    }

    state_->value_ = value;
    state_->ready_ = true;

    callbackAndNotify();
    return true;
  }

  bool SetFailed(const ErrorCode& error_code_) {
    Lock lock(state_->mutex_);

    if (state_->ready_) {
      return false;
    }

    state->error_code_ = error_code_;
    state->ready_ = true;
    state->failed_ = true;

    callbackAndNotify();
    return true;
  }

  bool IsReady() const { return state_->ready_; }

  bool IsFailed() const { return state_->failed_; }

  Future<Value> GetFuture() const { return Future<Value>(state_); }

 private:
  void callbackAndNotify() {
    for (auto callback : callbacks_) {
      callback(state_->error_code_, state_->value_);
    }
    state_->callbacks_.clear();
    state_->condition.notify_all();
  }

 private:
  FutureInnerStatePtr state_;
  typedef std::unique_lock<std::mutex> Lock;
  using FutureCallBackFunc = std::function < void(ErrorCode, const Value&);
};

} /* namespace tubemq */

#endif /* _TUBEMQ_FUTURE_H_ */
