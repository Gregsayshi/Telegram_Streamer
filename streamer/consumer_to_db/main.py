
import json
import threading
from broker import init_kafka_consumer, KafkaTopics
from database import *
import logging
import os

KAFKA_BROKER_INSTANCES = os.environ["KAFKA_BROKER_INSTANCES"].split(",")
LOG_LEVEL = os.environ["LOG_LEVEL"]
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()
if LOG_LEVEL:
    level_name = logging.getLevelName(LOG_LEVEL.upper())
    logger.setLevel(level_name)

lock = threading.Lock()
class SingletonMeta(type):
    """ Singleton metaclass - Implemented with a lock to ensure thread safety."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonConsumer(metaclass=SingletonMeta):
    """ Singleton class - Implementing kafka consumer in a singleton class to ensure only one instance of consumer is open at a time."""
    def get_consumer(self,topic):
        consumer = init_kafka_consumer(KAFKA_BROKER_INSTANCES, topic)
        return consumer
    pass



def consume_msgs(topic):
    sglcr = SingletonConsumer()
    consumer = sglcr.get_consumer(topic)
    if not consumer:
        logger.error(f"Failed to initialize a consumer for the topic {topic}")
        return

    for msg in consumer:
        try:
            msg_json = json.loads(msg.value)
            if topic == KafkaTopics.TGMESSAGES.value:
                logger.info(f"Retrieved a telegram message from kafka: {msg_json}")
                insert_message(msg_json)
            elif topic == KafkaTopics.TGCHANNEL_EVENTS.value:
                logger.info(f"Retrieved a telegram channel / chat event from kafka: {msg_json}")
                insert_chat_event(msg_json)
            elif topic == KafkaTopics.TGCHANNEL_LINKS.value:
                logger.info(f"Retrieved a telegram invitational link from kafka: {msg_json}")
                insert_link_data(msg_json, msg=None, channel_entity=None, from_config=False)
            elif topic == KafkaTopics.TGCHANNEL_INFO.value:
                logger.info(f"Retrieved a telegram channel information from kafka: {msg_json}")
                insert_channel_descr(msg_json)
            elif topic == KafkaTopics.CLIENT_INFO.value:
                logger.info(f"Retrieved a client information from kafka: {msg_json}")
                insert_client_info(msg_json)
            elif topic == KafkaTopics.VIEWCOUNT_UPDATE.value:
                logger.info(f"Retrieved a tg channel post viewcount update from kafka: {msg_json}")
                update_view_count(msg_json)
        except Exception as e:
            logger.error(f"Failed to store a msg from topic {topic}, with the following error: {e}")

def main():
    tgmessages_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.TGMESSAGES.value]))
    tgmessages_thread.start() #messages
    tgchannel_events_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.TGCHANNEL_EVENTS.value]))
    tgchannel_events_thread.start() #events
    tgchannel_info_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.TGCHANNEL_INFO.value]))
    tgchannel_info_thread.start() #channel info and descr
    tgchannel_links_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.TGCHANNEL_LINKS.value]))
    tgchannel_links_thread.start() #channel invite links
    tgchannel_participants_count_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.TGCHANNEL_PARTICIPANTS_COUNT.value]))
    tgchannel_participants_count_thread.start() #channel participants count
    client_info_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.CLIENT_INFO.value]))
    client_info_thread.start() #channel participants count
    client_info_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.VIEWCOUNT_UPDATE.value]))
    client_info_thread.start() #channel participants count


if __name__ == '__main__':
    main()
