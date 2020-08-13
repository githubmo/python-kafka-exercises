from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


c = AvroConsumer({
    'bootstrap.servers': 'localhost:32772,localhost:32773,localhost:32774',
    'group.id': 'avro-consumer',
    'schema.registry.url': 'http://localhost:8081',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['my_topic'])

while True:
    try:
        msg = c.poll(1.0)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

c.close()