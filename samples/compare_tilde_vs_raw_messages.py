#!/usr/bin/python3

import argparse

from pathnode_vis.pub_info import (
    PubInfos,
    PubInfo
)
from pathnode_vis.pubinfo_traverse import (
    InputSensorStampSolver,
    TopicGraph
)


def time2str(t):
    """
    t: builtin_interfaces.msg.Time
    """
    return f"{t.sec}.{t.nanosec:09d}"


def time2int(t):
    """
    t: builtin_interfaces.msg.Time
    """
    return t.sec * 10**9 + t.nanosec


def _rosbag_iter(rosbag_path):
    from rclpy.serialization import deserialize_message
    from rosidl_runtime_py.utilities import get_message
    import rosbag2_py

    storage_option = rosbag2_py.StorageOptions(rosbag_path, 'sqlite3')

    converter_option = rosbag2_py.ConverterOptions('cdr', 'cdr')
    reader = rosbag2_py.SequentialReader()

    reader.open(storage_option, converter_option)

    topic_types = reader.get_all_topics_and_types()
    type_map = {topic_types[i].name: topic_types[i].type
                for i in range(len(topic_types))}

    while reader.has_next():
        (topic, data, t) = reader.read_next()
        msg_type = get_message(type_map[topic])
        msg = deserialize_message(data, msg_type)
        yield topic, msg, t


def read_msgs(rosbag_path):
    """
    Read demo messages from rosbag

    Returns
    -------
    dictionary such as
    {
       <topic_name>: [<message>]
    }
    """
    from collections import defaultdict
    msg_topics = defaultdict(list)
    for topic, msg, t in _rosbag_iter(rosbag_path):
        msg_topics[topic].append(msg)
    return msg_topics


def read_pubinfo(raw_msgs):
    pubinfos = PubInfos()
    for i in range(5):
        msgs = raw_msgs[f"/topic{i+1}/info/pub"]
        for msg in msgs:
            pubinfos.add(PubInfo.fromMsg(msg))
    return pubinfos


def get_uuid2msg(raw_msgs):
    hashed_msgs = {}
    hash_target_keys = ['/topic1', '/topic2', '/topic3', '/topic4', '/topic5']
    for hash_target_key in hash_target_keys:
        hashed_msgs.update({
            msg.uuid: msg for msg in raw_msgs[hash_target_key]
        })
    return hashed_msgs


##############################
# checker functions
##############################

def check_number_of_raw_message_vs_pub_info(raw_msgs, pub_infos):
    """
    Compare number of messages

    Returns
    -------
    bool
    """
    ret = True

    for i in range(5):
        topic = f"/topic{i+1}"
        info = topic + "/info/pub"

        n_raw_msgs = len(raw_msgs[topic])
        n_pubinfo_msgs = len(raw_msgs[info])
        n_pubinfo_stamps = len(pub_infos.stamps(topic))

        if (n_raw_msgs != n_pubinfo_msgs or
                n_raw_msgs != n_pubinfo_stamps):
            ret = False

    return ret


def check_one_msg_and_pubinfo(
        raw_msgs, pub_infos, msg_idx):
    """
    Check specified raw_msg and pub_infos

    Returns
    -------
    bool

    Details
    -------
    (1) compare input stamps of trace.used_uuids and pubinfo
    (2) check overhead by comparing msg.steady and pub_time
    """
    topic = "/topic5"

    a_msg = raw_msgs[topic][msg_idx]
    stamp = time2str(a_msg.header.stamp)

    uuid2msg = get_uuid2msg(raw_msgs)

    graph = TopicGraph(pub_infos)
    solver = InputSensorStampSolver(graph)
    tilde_path = solver.solve2(pub_infos, topic, stamp)

    # get input stamps from raw message
    node2topic = {
        'sensor_dummy_node': '/topic1',
        'filter_node': '/topic2',
        'message_driven_node': '/topic3',
        'timer_driven_node': '/topic4',
        'actuator_dummy_node': '/topic5'
        }
    callback_traces = list(
        filter(lambda t: t.trace_type == 'callback_start', a_msg.traces)
    )
    trace_topic2input_stamps = {
        f"/topic{i+1}": [] for i in range(5)
    }
    for t in callback_traces:
        _topic = node2topic[t.node_name]
        for uuid in t.used_uuids:
            if uuid not in uuid2msg.keys():
                print(f"{_topic}: input {uuid} not found")
                continue
            trace_topic2input_stamps[_topic].append(
                time2int(uuid2msg[uuid].header.stamp)
            )

    # get input stamps from tilde
    def get_name_and_input_stamps(node):
        name = node.name
        stamps = []
        for d in node.data:
            for inputs in d.in_infos.values():
                # in_infos contains "input topic name" and "stamp"
                # ignore the former because of simplicity
                stamps.extend(
                    [time2int(i.stamp) for i in inputs]
                )
        return (name, stamps)

    pubinfo_topic2input_stamps = {
        f"/topic{i+1}": [] for i in range(5)
    }
    for (name, stamps) in tilde_path.apply(get_name_and_input_stamps):
        pubinfo_topic2input_stamps[name].extend(stamps)

    # compare
    ret = True
    for i in range(5):
        tgt = f"/topic{i+1}"
        trace_stamps = trace_topic2input_stamps[tgt]
        # tilde list up duplicated entries
        tilde_stamps = list(set(pubinfo_topic2input_stamps[tgt]))

        if sorted(trace_stamps) != sorted(tilde_stamps):
            ret = False

        if ret is False:
            print(tgt)
            print("trace_stamps:")
            print(trace_stamps)
            print("tilde_stamps:")
            print(tilde_stamps)

    return ret


def check_traces_and_pubinfo(raw_msgs, pub_infos, skips=40):
    """
    Compare raw_msg.traces and pubinfo.

    Parameters
    ----------
    raw_msgs
    pub_info
    skips

    Returns
    -------
    bool
    """
    ret = True
    for _i in range(len(raw_msgs["/topic5"][skips:])):
        i = _i + skips
        try:
            r = check_one_msg_and_pubinfo(
                raw_msgs, pub_infos, i)
        except Exception as e:
            print(f"{i}: Exception {type(e)}")
            raise
        if not r:
            print(f"{i}: NG")
            ret = False
    return ret


def main(args):
    bagfile = args.rosbag_folder
    raw_msgs = read_msgs(bagfile)
    pub_infos = read_pubinfo(raw_msgs)

    print("checking...")
    ret = True
    if not check_number_of_raw_message_vs_pub_info(raw_msgs, pub_infos):
        ret = False
        print("check_number_of_raw_message_vs_pub_info failed")
    if not check_traces_and_pubinfo(raw_msgs, pub_infos):
        ret = False
        print("check_traces_and_pubinfo failed")

    print("OK" if ret else "failed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("rosbag_folder")
    args = parser.parse_args()

    main(args)
