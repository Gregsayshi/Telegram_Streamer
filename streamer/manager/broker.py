from enum import Enum
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import logging

logger = logging.getLogger()


class KafkaTopics(Enum):
    TGINSTRUCTIONS = "tginstructions"
    TGCLIENT_FEEDBACK = "tgclient_feedback"
    LINK_FEEDBACK = "link_feedback"
    TGCHANNEL_LINKS = "tgchannel_links"
    VIEWCOUNT_UPDATE = "viewcount_update"

def init_kafka_producer(brokers):
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None
            )
        except Exception as e:
            logger.error(f"Couldn't connect to a broker: {e}")
        finally:
            if producer is None:
                logger.error("Retrying to connect a producer to a broker in 10s...")
                time.sleep(10)

    if producer:
        logger.info("Producer connected to a broker")

    return producer


def init_kafka_consumer(brokers, topic):
    consumer = None
    while consumer is None:
        #time.sleep(10)
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=brokers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: x.decode("utf-8"))
        except Exception as e:
            logger.error(f"Couldn't connect to a broker: {e}")
        finally:
            if consumer is None:
                logging.error("Retrying to connect to a broker in 10s...")

    if consumer:
        logger.info("Connected to a broker")

    return consumer
