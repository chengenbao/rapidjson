/*****************************************************
Copyright [20]<Copyright Owner>

//  this file is created by Goson 
//  time: 2016.04.26
//  version: 1.0
//  description: tube TDMsg define
//
//  time: 2020.09.10
//  version: 1.1
//  description: change the class's Code style
//
*****************************************************/

#ifndef TUBEMQ_CLIENT_TDMSG_H_
#define TUBEMQ_CLIENT_TDMSG_H_

#include <stdio.h>
#include <stdint.h>
#include <list>
#include <map>
#include <vector>
#include <string>


namespace tubemq {

using std::list;
using std::map;
using std::vector;
using std::string;


class DataItem {
 public:
  DataItem();
  DataItem(const DataItem& target);
  DataItem(const uint32_t length, const char* data);
  virtual ~DataItem();
  DataItem& operator=(const DataItem& target);
  const uint32_t GetLength() const { return length_; }
  const char* GetData() const { return data_; }

 private:
  void clearData();
  void copyData(const char* data, uint32_t length);

 private:
  uint32_t length_;
  char* data_;
};

class TubeMQTDMsg {
 public:
  TubeMQTDMsg();
  virtual ~TubeMQTDMsg();
  bool ParseTDMsg(const char* data,
    uint32_t data_length, string& err_info);
  bool ParseTDMsg(const vector<char>& data_vec, string& err_info);
  void Clear();
  const int32_t GetVersion() const { return version_; }
  bool IsNumBid() const { return is_numbid_; }
  const uint32_t GetAttrCount() const { return attr_count_; }
  const int64_t GetCreateTime() const { return create_time_; }
  bool ParseAttrValue(string attr_value,
    map<string, string>& result, string& err_info);
  const map<string, list<DataItem> >& GetAttr2DataMap() const { return attr2data_map_; }

 private:
  bool addDataItem2Map(const string& datakey, const DataItem& data_item);
  bool parseDefaultMsg(const char* data, uint32_t data_length,
    int32_t start_pos, string& err_info);
  bool parseMixAttrMsg(const char* data, uint32_t data_length,
    int32_t start_pos, string& err_info);
  bool parseBinMsg(const char* data, uint32_t data_length,
    int32_t start_pos, string& err_info);

 private:
  bool is_parsed_;
  bool is_numbid_;
  int32_t version_;
  int64_t create_time_;
  uint32_t msg_count_;
  uint32_t attr_count_;
  map<string, list<DataItem> > attr2data_map_;
};

}  // namespace tubemq


#endif  // TUBEMQ_CLIENT_TDMSG_H_

