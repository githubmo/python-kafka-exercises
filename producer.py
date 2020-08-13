#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import random

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

if __name__ == '__main__':

    topic = 'temperature'

    admin_client = AdminClient({'bootstrap.servers': 'localhost:32768,localhost:32769,localhost:32770'})

    admin_client.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': 'localhost:32768,localhost:32769,localhost:32770'
    })

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    cities = ['London', 'New York', 'Madrid', 'Paris']
    temps = [10, 20, 30]

    for n in range(10):
        city = random.choice(cities)
        temp = random.choice(temps)
        j = json.dumps({'city': city, 'temp': temp})
        record_key = city
        record_value = json.dumps(j)
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
