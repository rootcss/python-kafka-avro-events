import avro.datafile
from confluent_kafka import Producer
from utils import json_to_avro

TOPIC = 'simpl_events_protobuf'

def kafka_delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n' % err)
    else:
        print('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))


if __name__ == '__main__':
    user_schema = avro.schema.Parse(open("./schemas/user.avsc", "rb").read())

    conf = {'bootstrap.servers': 'localhost:9092'}
    kafka = Producer(**conf)

    event = {'name': 'Eli', 'favorite_number': 42, 'favorite_color': 'black'}
    avro_event = json_to_avro(event, user_schema)

    kafka.produce(TOPIC, avro_event, callback=kafka_delivery_callback)
    kafka.poll(0)
    print("Waiting for kafka deliveries..")
    kafka.flush()
