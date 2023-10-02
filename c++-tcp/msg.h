#ifndef MSG_H_
#define MSG_H_

#include "json.h"

using nlohmann::json;

enum ResponseType { OK, REJECT };

enum CommandType { GET, PUT, DEL };

enum InstanceState { INPROGRESS, COMMITTED, EXECUTED };

enum MessageType { PREPAREREQUEST, PREPARERESPONSE, ACCEPTREQUEST, 
                   ACCEPTRESPONSE, COMMITREQUEST, COMMITRESPONSE };

struct Command {
  Command() = default;
  Command(const Command&) = default;
  Command(Command&&) = default;
  Command& operator=(Command&&) = default;

  Command(CommandType type, std::string key, std::string value) 
      : type_(type), key_(key), value_(value) {}
  CommandType type_;
  std::string key_;
  std::string value_;

  // void to_json(json& j, const Command& c) {
  //   j = json{ {"type_", c.type_}, {"key_", c.key_}, {"value_", c.value_}};
  // }

  // void from_json(json& j, Command& c) {
  //   c.type_ = j.at("type_").get<CommandType>();
  //   c.key_ = j.at("key_").get<std::string>();
  //   c.value_ = j.at("value_").get<std::string>();
  // }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Command, type_, key_, value_)

struct Instance {
  Instance() = default;
  Instance(const Instance&) = default;
  Instance(Instance&&) = default;
  Instance& operator=(Instance&&) = default;

  explicit Instance(int64_t ballot, 
  	      int64_t index, 
  	      int64_t client_id, 
  	      InstanceState state, 
  	      Command command)
      : ballot_(ballot),
        index_(index),
        client_id_(client_id),
        state_(state),
        command_(std::move(command)) {}

  int64_t ballot_;
  int64_t index_;
  int64_t client_id_;
  InstanceState state_;
  Command command_;

  // void to_json(json& j, const Instance& i) {
  //   j = json{ {"ballot_", i.ballot_}, {"index_", i.index_},
  //             {"client_id_", i.client_id_}, {"state_", i.state_},
  //             {"command_", {"type_", i.command_.type_}, 
  //                          {"key_", i.command_.key_}, 
  //                          {"value_", i.command_.value_}}};
  // }

  // void from_json(json& j, Instance& i) {
  //   i.ballot_ = j.at("ballot_").get<int64_t>();
  //   i.index_ = j.at("index_").get<int64_t>();
  //   i.client_id_ = j.at("client_id_").get<int64_t>();
  //   i.state_ = j.at("state_").get<InstanceState>();
  //   i.command_.type_ = j.at("command_").at("type_").get<CommandType>();
  //   i.command_.key_ = j.at("command_").at("key_").get<std::string>();
  //   i.command_.value_ = j.at("command_").at("value_").get<std::string>();
  // }

};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Instance, ballot_, index_, client_id_, state_, command_)

struct Message {
  Message(MessageType type, int64_t channel_id, std::string msg)
      : type_(type), channel_id_(channel_id), msg_(msg) {}
  MessageType type_;
  int64_t channel_id_;
  std::string msg_;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Message, type_, channel_id_, msg_)

struct PrepareRequest {
  PrepareRequest() = default;
  PrepareRequest(int64_t ballot, int64_t sender) 
      : ballot_(ballot), sender_(sender) {}
  int64_t ballot_;
  int64_t sender_;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(PrepareRequest, ballot_, sender_)

struct PrepareResponse {
  PrepareResponse() = default;
  PrepareResponse(ResponseType type, 
  	              int64_t ballot, 
  	              std::vector<Instance> instances)
      : type_(type), ballot_(ballot), instances_(instances) {}
  ResponseType type_;
  int64_t ballot_;
  std::vector<Instance> instances_;

  void to_json(json& j, const PrepareResponse& p) {
    j = json{ {"type_", p.type_}, {"ballot_", p.ballot_}, {"instances_", p.instances_} };
  }

};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(PrepareResponse, type_, ballot_, instances_)

struct AcceptRequest {
  AcceptRequest(Instance instance, int64_t sender) 
      : instance_(std::move(instance)), sender_(sender) {}
  Instance instance_;
  int64_t sender_;

  void to_json(json& j, const AcceptRequest& p) {
    j = json{ {"instance_", {
                  {"ballot_", p.instance_.ballot_}, 
                  {"index_", p.instance_.index_},
                  {"client_id_", p.instance_.client_id_}, 
                  {"state_", p.instance_.state_}, 
                  {"command_", {"type_", p.instance_.command_.type_},
                      {"key_", p.instance_.command_.key_}, 
                      {"value_", p.instance_.command_.value_}}}},
              {"sender_", p.sender_}};
  }

};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(AcceptRequest, instance_, sender_)

struct AcceptResponse {
  AcceptResponse() = default;
  AcceptResponse(ResponseType type, int64_t ballot) 
      : type_(type), ballot_(ballot) {}
  ResponseType type_;
  int64_t ballot_;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(AcceptResponse, type_, ballot_)

struct CommitRequest {
  CommitRequest(int64_t ballot, 
  	            int64_t last_executed, 
  	            int64_t global_last_executed, 
  	            int64_t sender) 
      : ballot_(ballot), 
        last_executed_(last_executed),
        global_last_executed_(global_last_executed),
        sender_(sender) {}
  int64_t ballot_;
  int64_t last_executed_;
  int64_t global_last_executed_;
  int64_t sender_;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(CommitRequest, ballot_, last_executed_, 
	                               global_last_executed_, sender_)

struct CommitResponse {
  CommitResponse() = default;
  CommitResponse(ResponseType type, int64_t ballot, int64_t last_executed)
      : type_(type), ballot_(ballot), last_executed_(last_executed) {}
  ResponseType type_;
  int64_t ballot_;
  int64_t last_executed_;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(CommitResponse, type_, ballot_, last_executed_)

#endif
