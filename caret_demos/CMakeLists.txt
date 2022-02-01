cmake_minimum_required(VERSION 3.8)
project(caret_demos)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake_auto REQUIRED)
ament_auto_find_build_dependencies()

set(msg_files
  "msg/Trace.msg"
  "msg/Traces.msg"
  DEPENDENCIES std_msgs builtin_interfaces
)

rosidl_generate_interfaces(${PROJECT_NAME}
  ${msg_files}
)

ament_export_dependencies(rosidl_default_runtime)

ament_auto_add_executable(end_to_end_sample
  src/end_to_end_sample.cpp)

target_link_libraries(end_to_end_sample
  uuid
)

rosidl_target_interfaces(end_to_end_sample
  ${PROJECT_NAME} "rosidl_typesupport_cpp")

ament_target_dependencies(end_to_end_sample rclcpp)
install(TARGETS
  end_to_end_sample
  DESTINATION lib/${PROJECT_NAME})

# install launch files
install(DIRECTORY
  launch
  DESTINATION share/${PROJECT_NAME}
)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
endif()

ament_auto_package()
