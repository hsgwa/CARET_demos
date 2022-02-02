#include <chrono>
#include <memory>

#include <rclcpp/rclcpp.hpp>
#include <rosgraph_msgs/msg/clock.hpp>

using namespace std::chrono_literals;

class ClockPublisher : public rclcpp::Node {
 public:
  ClockPublisher() : Node("clock_publisher") {
    double hz;
    declare_parameter<double>("hz", 10.0);
    get_parameter<double>("hz", hz);

    double rate;
    declare_parameter<double>("rate", 1.0);
    get_parameter<double>("rate", rate);

    auto period = std::chrono::duration<double>(1.0/hz);

    auto timer_callback =
        [&, period, rate]() {
          auto msg = std::make_unique<rosgraph_msgs::msg::Clock>();

          time_ = time_ + rclcpp::Duration(period * rate);
          msg->clock = time_;

          pub_->publish(std::move(msg));
        };

    time_ = this->now();
    pub_ = create_publisher<rosgraph_msgs::msg::Clock>("/clock", 10);
    timer_ = create_wall_timer(period, timer_callback);
  }

  rclcpp::TimerBase::SharedPtr timer_;
  rclcpp::Publisher<rosgraph_msgs::msg::Clock>::SharedPtr pub_;
  rclcpp::Time time_;
};

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);

  rclcpp::spin(std::make_shared<ClockPublisher>());
  rclcpp::shutdown();

  return 0;
}