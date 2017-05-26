from __future__ import print_function

import calendar
import datetime as dt
import sys
import time

import tabulate

import pykafka
from pykafka.common import OffsetType
from pykafka.protocol import PartitionOffsetCommitRequest
from pykafka.utils.compat import PY3, iteritems

def print_consumer_lag(client, topic, consumer_group):
    """Print lag for a topic/consumer group.
    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`str`
    :param consumer_group: Name of the consumer group to fetch offsets for.
    :type consumer_groups: :class:`str`
    """
    # Don't auto-create topics.
    if topic not in client.topics:
        raise ValueError('Topic {} does not exist.'.format(topic))
    topic = client.topics[topic]

    lag_info = fetch_consumer_lag(client, topic, consumer_group)
    lag_info = [(k, '{:,}'.format(v[0] - v[1]), v[0], v[1])
                for k, v in iteritems(lag_info)]
    print(tabulate.tabulate(
        lag_info,
        headers=['Partition', 'Lag', 'Latest Offset', 'Current Offset'],
        numalign='center',
    ))

    total = sum(int(i[1].replace(',', '')) for i in lag_info)
    print('\n Total lag: {:,} messages.'.format(total))


def fetch_consumer_lag(client, topic, consumer_group):
    """Get raw lag data for a topic/consumer group.
    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`pykafka.topic.Topic`
    :param consumer_group: Name of the consumer group to fetch lag for.
    :type consumer_groups: :class:`str`
    :returns: dict of {partition_id: (latest_offset, consumer_offset)}
    """
    latest_offsets = fetch_offsets(client, topic, 'latest')
    consumer = topic.get_simple_consumer(consumer_group=consumer_group,
                                         auto_start=False)
    current_offsets = consumer.fetch_offsets()
    return {p_id: (latest_offsets[p_id].offset[0], res.offset)
            for p_id, res in current_offsets}


def fetch_offsets(client, topic, offset):
    """Fetch raw offset data from a topic.
    :param client: KafkaClient connected to the cluster.
    :type client:  :class:`pykafka.KafkaClient`
    :param topic:  Name of the topic.
    :type topic:  :class:`pykafka.topic.Topic`
    :param offset: Offset to fetch. Can be earliest, latest or a datetime.
    :type offset: :class:`pykafka.common.OffsetType` or
        :class:`datetime.datetime`
    :returns: {partition_id: :class:`pykafka.protocol.OffsetPartitionResponse`}
    """
    if offset.lower() == 'earliest':
        return topic.earliest_available_offsets()
    elif offset.lower() == 'latest':
        return topic.latest_available_offsets()
    else:
        offset = dt.datetime.strptime(offset, "%Y-%m-%dT%H:%M:%S")
        offset = int(calendar.timegm(offset.utctimetuple())*1000)
        return topic.fetch_offset_limits(offset)

def main():
    print(len(sys.argv))
    if len(sys.argv) <3 or len(sys.argv) >4:
        print('Usage is script.py host:port topic consumer-group(optional, default value is default-consumer)')
        sys.exit(0)
    consumer_group="default-consumer"
    if len(sys.argv)==3:
        client = pykafka.KafkaClient(sys.argv[1])
        topic = sys.argv[2]
    else:
        client = pykafka.KafkaClient(sys.argv[1])
        topic = sys.argv[2]
        consumer_group=sys.argv[3]
    print_consumer_lag(client,topic,consumer_group)

if __name__ == '__main__':
    main()
