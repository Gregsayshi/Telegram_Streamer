from kafka import KafkaConsumer
from enum import Enum
import logging
import time

logger = logging.getLogger()


class KafkaTopics(Enum):
    TGMESSAGES = "tgmessages"
    RECENT_TGMESSAGES = "recent_tgmessages"
    TGCHANNEL_EVENTS = "tgchannel_events"
    TGCHANNEL_INFO = "tgchannel_info"
    TGCHANNEL_LINKS = "tgchannel_links"
    TGCHANNEL_PARTICIPANTS_COUNT = "tgchannel_participants_count"
    CLIENT_INFO = "client_info"
    VIEWCOUNT_UPDATE = "viewcount_update"


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
                value_deserializer=lambda x: x.decode("utf-8"))
        except Exception as e:
            logger.error(f"Couldn't connect to a broker: {e}")
        finally:
            if consumer is None:
                logger.info("Retrying to connect to a broker in 10s...")

    if consumer:
        logger.info("Connected to a broker")

    return consumer
