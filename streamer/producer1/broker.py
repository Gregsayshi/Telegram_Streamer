from kafka import KafkaConsumer, KafkaProducer
from enum import Enum
import json
import time
import logging

logger = logging.getLogger()


class KafkaTopics(Enum):
    TGMESSAGES = "tgmessages"
    RECENT_TGMESSAGES = "recent_tgmessages"
    TGCHANNEL_EVENTS = "tgchannel_events"
    TGCHANNEL_INFO = "tgchannel_info"
    TGCHANNEL_LINKS = "tgchannel_links" #tg links we are actively interested in
    TGINSTRUCTIONS = "tginstructions"
    TGCLIENT_FEEDBACK = "tgclient_feedback"
    TGCHANNEL_PARTICIPANTS_COUNT = "tgchannel_participants_count"
    LINK_FEEDBACK = "link_feedback"
    CLIENT_INFO = "client_info"
    VIEWCOUNT_UPDATE = "viewcount_update"

def init_kafka_producer(brokers):
    producer = None
    while producer is None:
        time.sleep(10)
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: json.dumps(k).encode("utf-8")
            )
        except Exception as e:
            logger.error(f"Couldn't connect to a broker: {e}")
        finally:
            if producer is None:
                logger.info("Retrying to connect to a broker in 10s...")

    if producer:
        logger.info("Connected to a broker")

    return producer


def init_kafka_consumer(brokers, topic):
    consumer = None
    while consumer is None:
        time.sleep(10)
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=brokers,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
                value_deserializer=lambda v: v.decode("utf-8"))
        except Exception as e:
            logger.error(f"Couldn't connect to a broker: {e}")
        finally:
            if consumer is None:
                logger.info("Retrying to connect to a broker in 10s...")

    if consumer:
        logger.info("Connected to a broker")

    return consumer
