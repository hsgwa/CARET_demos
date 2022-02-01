#include <chrono>
#include <memory>
#include <vector>
#include <random>
#include <uuid/uuid.h>
#include <unordered_set>
#include <tuple>

#include "rclcpp/rclcpp.hpp"
#include "caret_demos/msg/trace.hpp"
#include "caret_demos/msg/traces.hpp"
#include "pathnode/path_node.hpp"
#include "pathnode/sub_timing_advertise_node.hpp"
#include "pathnode/timing_advertise_publisher.hpp"

#define QOS_HISTORY_SIZE 10

using namespace std::chrono_literals;

#define PUBLISH_TYPE "publish"
#define CALLBACK_START "callback_start"

std::chrono::milliseconds lognormal_distribution(double max)
{
  static std::random_device seed_gen;
  static std::default_random_engine engine(seed_gen());
  static std::lognormal_distribution<> dist(1.1, 1.7);

  int sleep_ms = std::max(std::min(dist(engine), max), 20.0);
  return std::chrono::milliseconds(sleep_ms);
}


std::string generate_uuid(){
  uuid_t uuid;
  char uuid_s[100] = {};
  uuid_generate(uuid);
  uuid_unparse_upper(uuid, uuid_s);
  return std::string(uuid_s);
}

inline int64_t get_now(){
  return std::chrono::steady_clock::now().time_since_epoch().count();
}


inline int rand(int max) {
  static std::random_device rng;
  return rng() % max;
}

inline std::vector<int> rand_range(int max, size_t size) {
  std::unordered_set<int> l;
  while (l.size() < size) {
    auto r = rand(max);
    if(l.count(r) > 0) {
      continue;
    }
    l.insert(r);
  }

  return std::vector<int>(l.begin(), l.end());
}

inline std::unique_ptr<caret_demos::msg::Traces> generate_msg(
  int64_t seq,
  std::string node_name,
  std::string trace_type,
  rclcpp::Time header_stamp,
  std::vector<caret_demos::msg::Traces> source_messages
)
{
  auto msg = std::make_unique<caret_demos::msg::Traces>();
  msg->trace_type = trace_type;
  msg->steady_t = get_now();
  msg->seq = seq;
  msg->node_name = node_name;
  msg->header.stamp = header_stamp;
  msg->uuid = generate_uuid();

  if (source_messages.size() > 0) {
    msg->source_t_min = std::numeric_limits<int64_t>::max();
    msg->source_t_max = std::numeric_limits<int64_t>::min();
  }

  std::unordered_set<std::string> added_uuids{};

  for(auto &source_message : source_messages) {
    msg->used_uuids.push_back(source_message.uuid);
    msg->source_t_min = std::min(msg->source_t_min, source_message.source_t_min);
    msg->source_t_max = std::max(msg->source_t_max, source_message.source_t_max);

    for(auto &trace : source_message.traces) {
      if (added_uuids.count(trace.uuid) > 0) {
        continue;
      }
      added_uuids.insert(trace.uuid);
      msg->traces.push_back(trace);
    }

    caret_demos::msg::Trace trace;
    trace.uuid = source_message.uuid;
    trace.used_uuids =  source_message.used_uuids;
    trace.trace_type = source_message.trace_type;
    trace.steady_t = source_message.steady_t;
    msg->traces.push_back(trace);
  }

  msg->latency_min = msg->steady_t - msg->source_t_max;
  msg->latency_max = msg->steady_t - msg->source_t_min;
  return msg;
}

template<typename T>
class CircularBuffer
{
public:
 explicit CircularBuffer(size_t size)
 : size_(size)
 {
 }

 void push_back(T datum) {
   if (buf_.size() < size_) {
     buf_.push_back(datum);
   } else {
    int index = get_index(0);
    buf_[index] = datum;
   }
   forward_offset_index();
 }

 T get(int i) {
   int index = get_index(i);
   return  buf_[index];
 }

 std::vector<T> gets(std::vector<int> is) {
   std::vector<T> data;
   for(auto &i: is) {
     data.push_back(get(i));
   }
   return data;
 }

 size_t size() {
   return size_;
 }

 size_t count() {
   return buf_.size();
 }

private:

  int get_index(int i) {
   return (i + i_offset_) % size_;
  }

  void forward_offset_index() {
    i_offset_ = get_index(1);
  }

  std::vector<T> buf_;
  size_t size_;
  int i_offset_;
};

class TimerDependencyNode : public pathnode::SubTimingAdvertiseNode
{
public:
  TimerDependencyNode(
    std::string node_name, std::string sub_topic_name, std::string pub_topic_name,
    int period_ms)
  : SubTimingAdvertiseNode(node_name), seq_(0), seq__(0), buf_(5)
  {
    pub_ = create_timing_advertise_publisher<caret_demos::msg::Traces>(pub_topic_name, QOS_HISTORY_SIZE);
    sub_ = create_timing_advertise_subscription<caret_demos::msg::Traces>(
      sub_topic_name, QOS_HISTORY_SIZE,
      [&](caret_demos::msg::Traces::UniquePtr msg)
      {
        auto sub_msg = generate_msg(seq_++, get_name(), CALLBACK_START, now(), {*msg});
        buf_.push_back(*sub_msg);
        rclcpp::sleep_for(lognormal_distribution(45));
      });


    timer_ = create_wall_timer(
      std::chrono::milliseconds(period_ms), [&]()
      {
        rclcpp::sleep_for(lognormal_distribution(45));
        if (buf_.count() == buf_.size()) {
          auto used_msgs =  buf_.gets(rand_range(5, rand(4)));
          auto msg_ = generate_msg(seq__++, get_name(), PUBLISH_TYPE, now(), used_msgs);
          for (auto &used_msg: used_msgs) {
            pub_->add_explicit_input_info(
              sub_->get_topic_name(),
              used_msg.header.stamp
            );
          }
          pub_->publish(std::move(msg_));
        }
      });
  }

private:
  int64_t seq_;
  int64_t seq__;
  pathnode::TimingAdvertisePublisher<caret_demos::msg::Traces>::SharedPtr pub_;
  rclcpp::Subscription<caret_demos::msg::Traces>::SharedPtr sub_;
  CircularBuffer<caret_demos::msg::Traces> buf_;
  rclcpp::TimerBase::SharedPtr timer_;
};

class ActuatorDummy : public pathnode::SubTimingAdvertiseNode
{
public:
  ActuatorDummy(std::string node_name, std::string sub_topic_name)
  : SubTimingAdvertiseNode(node_name)
  {
    sub_ = create_timing_advertise_subscription<caret_demos::msg::Traces>(
      sub_topic_name, QOS_HISTORY_SIZE,
      [&](caret_demos::msg::Traces::UniquePtr msg)
      {
        rclcpp::sleep_for(lognormal_distribution(80));
        (void)msg;
      }
    );
  }

private:
  rclcpp::Subscription<caret_demos::msg::Traces>::SharedPtr sub_;
};

class NoDependencyNode : public pathnode::SubTimingAdvertiseNode
{
public:
  NoDependencyNode(std::string node_name, std::string sub_topic_name, std::string pub_topic_name)
  : SubTimingAdvertiseNode(node_name), seq_(0), seq__(0)
  {
    pub_ = create_timing_advertise_publisher<caret_demos::msg::Traces>(pub_topic_name, QOS_HISTORY_SIZE);
    sub_ = create_timing_advertise_subscription<caret_demos::msg::Traces>(
      sub_topic_name, QOS_HISTORY_SIZE, [&](caret_demos::msg::Traces::UniquePtr msg)
      {
        auto msg_cb_start = generate_msg(seq_++, get_name(), CALLBACK_START, now(), {*msg});
        msg_cb_start->header.stamp = msg->header.stamp;

        rclcpp::sleep_for(lognormal_distribution(200));

        auto msg_publish = generate_msg(seq__++, get_name(), PUBLISH_TYPE, now(), {*msg_cb_start});
        msg_publish->header.stamp = msg_cb_start->header.stamp;

        pub_->publish(std::move(msg_publish));
      });
  }

private:
  int64_t seq_;
  int64_t seq__;
  pathnode::TimingAdvertisePublisher<caret_demos::msg::Traces>::SharedPtr pub_;
  rclcpp::Subscription<caret_demos::msg::Traces>::SharedPtr sub_;
};

class SubDependencyNode : public pathnode::SubTimingAdvertiseNode
{
public:
  SubDependencyNode(
    std::string node_name,
    std::string sub_topic_name,
    std::string subsequent_sub_topic_name,
    std::string pub_topic_name
  )
  : SubTimingAdvertiseNode(node_name), seq_(0), seq__(0), buf_(5)
  {
    sub1_ = create_timing_advertise_subscription<caret_demos::msg::Traces>(
      sub_topic_name, QOS_HISTORY_SIZE, [&](caret_demos::msg::Traces::UniquePtr msg)
      {
        auto sub_msg = generate_msg(seq_++, get_name(), CALLBACK_START, now(), {*msg});
        buf_.push_back(*sub_msg);
        rclcpp::sleep_for(lognormal_distribution(45));
      });
    sub2_ = create_timing_advertise_subscription<caret_demos::msg::Traces>(
      subsequent_sub_topic_name, QOS_HISTORY_SIZE, [&](caret_demos::msg::Traces::UniquePtr msg)
      {
        (void) msg;
        rclcpp::sleep_for(lognormal_distribution(45));

        if (buf_.count() == buf_.size()) {
          auto used_msgs = buf_.gets({0,1,2,3,4});
          auto pub_msg = generate_msg(seq__++, get_name(), PUBLISH_TYPE, now(), used_msgs);
          for (auto &used_msg: used_msgs) {
            pub_->add_explicit_input_info(
              sub1_->get_topic_name(),
              used_msg.header.stamp
            );
          }
          pub_->publish(std::move(pub_msg));
        }
      });
    pub_ = create_timing_advertise_publisher<caret_demos::msg::Traces>(pub_topic_name, QOS_HISTORY_SIZE);
  }

private:
  int64_t seq_;
  int64_t seq__;
  pathnode::TimingAdvertisePublisher<caret_demos::msg::Traces>::SharedPtr pub_;
  rclcpp::Subscription<caret_demos::msg::Traces>::SharedPtr sub1_;
  rclcpp::Subscription<caret_demos::msg::Traces>::SharedPtr sub2_;
  CircularBuffer<caret_demos::msg::Traces> buf_;
};


class SensorDummy : public pathnode::SubTimingAdvertiseNode
{
public:
  SensorDummy(std::string node_name, std::string topic_name, int period_ms)
  : SubTimingAdvertiseNode(node_name), seq_(0)
  {
    auto period = std::chrono::milliseconds(period_ms);

    auto callback = [&]() {
        rclcpp::sleep_for(lognormal_distribution(40));
        auto msg = generate_msg(seq_++, get_name(), PUBLISH_TYPE, now(), {});
        msg->source_t_min = msg->steady_t;
        msg->source_t_max = msg->steady_t;
        pub_->publish(std::move(msg));
      };
    pub_ = create_timing_advertise_publisher<caret_demos::msg::Traces>(topic_name, QOS_HISTORY_SIZE);
    timer_ = create_wall_timer(period, callback);
  }

private:
  pathnode::TimingAdvertisePublisher<caret_demos::msg::Traces>::SharedPtr pub_;
  int64_t seq_;
  rclcpp::TimerBase::SharedPtr timer_;
};

int main(int argc, char * argv[])
{
  rclcpp::init(argc, argv);

  auto executor = std::make_shared<rclcpp::executors::MultiThreadedExecutor>();

  std::vector<std::shared_ptr<rclcpp::Node>> nodes;

  nodes.emplace_back(std::make_shared<ActuatorDummy>("actuator_dummy_node", "/topic4"));
  nodes.emplace_back(
    std::make_shared<NoDependencyNode>("filter_node", "/topic1", "/topic2"));
  nodes.emplace_back(
    std::make_shared<SubDependencyNode>("message_driven_node", "/topic2", "/drive", "/topic3"));
  nodes.emplace_back(
    std::make_shared<TimerDependencyNode>("timer_driven_node", "/topic3", "/topic4", 20)); // 50Hz
  nodes.emplace_back(std::make_shared<SensorDummy>("sensor_dummy_node", "/topic1", 25)); // 40Hz
  nodes.emplace_back(std::make_shared<SensorDummy>("drive_node", "/drive", 100)); // 10Hz

  for (auto & node : nodes) {
    executor->add_node(node);
  }

  executor->spin();

  return 0;
}
