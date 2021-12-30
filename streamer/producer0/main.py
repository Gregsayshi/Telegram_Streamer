import json
from broker import *
import logging
import os
from telethon import TelegramClient, events
from datetime import datetime as dt
import asyncio
import logging
import telegram_functions
from telegram_functions import *
import time
import threading
from parsers import parse_message, parse_chat_event, get_dialog_entity_id, DATE_FORMAT
import yaml
import os
import re



VIEW_COUNT_DELAY1_S = 300
VIEW_COUNT_DELAY2_S = 1200
VIEW_COUNT_DELAY3_S = 3600
PERIODIC_CHECK_INTERVAL_S = MAX_CHANNELS_PER_CLIENT = None
DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%m"
CLIENT_ID = os.environ["CLIENT_ID"]
KAFKA_BROKER_INSTANCES = os.environ["KAFKA_BROKER_INSTANCES"].split(",")
LOG_LEVEL = os.environ["LOG_LEVEL"]
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()
if LOG_LEVEL:
    level_name = logging.getLevelName(LOG_LEVEL.upper())
    logger.setLevel(level_name)

#Connect with TG client

class SingletonMeta(type):
    """ Singleton metaclass """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class SingletonProducer(metaclass=SingletonMeta):
    """ Singleton class - Implementing kafka producer in a singleton class to ensure only one instance of producer is open at a time."""
    def get_producer(self):
        producer = init_kafka_producer(KAFKA_BROKER_INSTANCES)
        return producer
    pass

class SingletonConsumer(metaclass=SingletonMeta):
    """ Singleton class - Implementing kafka consumer in a singleton class to ensure only one instance of consumer is open at a time."""
    def get_consumer(self):
        consumer = init_kafka_consumer(KAFKA_BROKER_INSTANCES,KafkaTopics.TGINSTRUCTIONS.value)
        return consumer
    pass


def get_config():
    global VIEW_COUNT_DELAY1_S, VIEW_COUNT_DELAY2_S, VIEW_COUNT_DELAY3_S, MAX_CHANNELS_PER_CLIENT
    global PERIODIC_CHECK_INTERVAL_S, CLIENT_ID
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            app_title = config["app_title"]
            phone_number = config["phone_number"]
            api_id = config["api_id"]
            api_hash = config["api_hash"]
            MAX_CHANNELS_PER_CLIENT = config["max_channels_per_client"]
            PERIODIC_CHECK_INTERVAL_S = config["periodic_check_interval_s"]
            CLIENT_ID = config["client_id"]

        logger.debug(f"Phone number: {phone_number}")
        logger.debug(f"Api id: {api_id}")
        logger.debug(f"Api hash: {api_hash}")
    except Exception as e:
        logger.error(f"Failed to read the app configuration with the following error: {e}")
    return app_title, phone_number, api_id, api_hash


app_title, phone_number, api_id, api_hash = get_config() #
client = TelegramClient(app_title, api_id, api_hash)
check = 0
wait_time = 5

def start_client(): #start client
    client.start()
    check = 1
    return check

while check == 0: #keept trying to start client if it doesnt work straight away
    check = start_client()
    if check != 0:
        break
    if wait_time > 43200: #43200 seconds = 12 hours
        break
    time.sleep(wait_time)
    wait_time = wait_time * 2
loop = asyncio.get_event_loop()
msg_count, event_count = 0, 0


async def selfcheck(client):
    myself = await client.get_me() #check if client is running after start
    return myself

client_info = {}
client_info["phone_number"] = phone_number
client_info["client_id"] = CLIENT_ID
client_info["processed_timestamp"] = time.time()


async def client_check(client,client_info):
    """Collects relevant information about client from TG API after client start-up.
       Sends data to Kafka."""
    alive = await client.get_me()
    if alive:
        logger.info(f"Client {CLIENT_ID} is active.")
        count = 0
        open_dialogs = await telegram_functions.getOpenDialogs(client)
        open_dialogs = len([dialog.entity.id for dialog in open_dialogs])
        client_info["open_dialogs"] = open_dialogs #count
        client_info["is_active"] = 1
    else:
        logger.info(f"Client {CLIENT_ID} is NOT active.")
        client_info["open_dialogs"] = None
        client_info["is_active"] = 0
    sglpr = SingletonProducer()
    producer = sglpr.get_producer()
    future = producer.send(KafkaTopics.CLIENT_INFO.value, client_info)
    result = future.get(timeout=10)  # Block until a single msg is sent
    return


async def bot_handler(event,captcha_type):
    """Solves Anti-Bot Captcha"""

    def numeric(equation): #helper function
        if '+' in equation:
            y = equation.split('+')
            y[1] = ''.join(c for c in y[1] if c.isdigit()) #if there is a "=" without space after number, remove it
            x = int(y[0])+int(y[1])
        elif '-' in equation:
            y = equation.split('-')
            y[1] = ''.join(c for c in y[1] if c.isdigit()) #if there is a "=" without space after number, remove it
            x = int(y[0])-int(y[1])
        elif '*' in equation:
            y = equation.split('-')
            y[1] = ''.join(c for c in y[1] if c.isdigit()) #if there is a "=" without space after number, remove it
            x = int(y[0])*int(y[1])
        return x

    if "button" in captcha_type: #click on captcha button - only clicks on FIRST button available in message, then stops
        try:
            buttons = await event.get_buttons()
            if buttons is not None:
                for buttonline in buttons:
                    for button in buttonline:
                        buttontxt = button.button.text
                        logger.info(f"Clickable button detected! Clicking button with the following button text: {buttontxt}")
                        await button.click()
                        break
                    break
            return
        except Exception as e:
            logger.error(f"Exception occured during button.click() @channel_message_handler(event) with the following ERROR: {e}", exc_info=True)
            return

    if "math_numbers" in captcha_type:
        try:
            message = event.message
            msg = message.message
            entity = message.peer_id.channel_id
            msg = re.sub('[a-zA-Z]', '', msg)
            result = numeric(msg) #solve math captcha
            await client.send_message(entity=entity,message=str(result)) #send answer to channel
            logger.info(f"Math Captcha detected! Sending the following captcha answer to channel: {result}")
            return
        except Exception as e:
            logger.error(f"Exception occured during match_numbers captcha @channel_message_handler(event) with the following ERROR: {e}", exc_info=True)
            return

    if "math_words" in captcha_type:
        try:
            message = event.message
            msg = message.message
            entity = message.peer_id.channel_id
            conf_nw = []
            number_words_dict = {"one":int(1),"two":int(2),"three":int(3),"four":int(4),"five":int(5),"six":int(6),"seven":int(7),"eight":int(8),"nine":int(9),"ten":int(10),\
            "One":int(1),"Two":int(2),"Three":int(3),"Four":int(4),"Five":int(5),"Six":int(6),"Seven":int(7),"Eight":int(8),"Nine":int(9),"Ten":int(10)}
            msg_arr = msg.split(" ")
            for word in msg_arr:
                if word in number_words_dict.keys():
                    conf_nw.append(number_words_dict[word])

            if len(conf_nw) > 1:
                if "plus" in msg or "PLUS" in msg or "Plus" in msg:
                    result = conf_nw[0] + conf_nw[1]
                if "minus" in msg or "MINUS" in msg or "Minus" in msg:
                    result = conf_nw[0] - conf_nw[1]
            else:
                logger.warning(f"Math captcha for the following captcha could not be solved: {msg}")
                return

            await client.send_message(entity=entity,message=str(result)) #send answer to channel
            logger.info(f"Math Captcha detected! Sending the following captcha answer to channel: {result}")
            return

        except Exception as e:
            logger.error(f"Exception occured during match_words captcha @channel_message_handler(event) with the following ERROR: {e}", exc_info=True)
            return


@client.on(events.NewMessage)
async def channel_message_handler(event):
    """ Processes a TG message - triggers when client receives new message
        attr - event: new Telegram message recorded by Telethon API """
    try:
        sglpr = SingletonProducer()
        producer = sglpr.get_producer()
        global msg_count
        if msg_count % 100 == 0:
            logger.info(f"Recorded a message - logging once every 100 msgs")
        msg_count += 1
        msg = event.message
        if "KeyboardButtonCallback" in str(msg):
            captcha_type = "button"
            await bot_handler(event,captcha_type)
        operations = ["+","-","*"]
        if any(exist in str(msg.message) for exist in operations) and "=" in str(msg.message):
            captcha_type = "math_numbers"
            await bot_handler(event,captcha_type)
        number_words = ["one","two","three","four","five","six","seven","eight","nine","ten","One","Two","Three","Four","Five","Six","Seven","Eight","Nine","Ten",\
        "ONE","TWO","THREE","FOUR","FIVE","SIX","SEVEN","EIGHT","NINE","TEN"]
        operation_words = ["plus","minus","Plus","Minus","PLUS","MINUS"]
        if any(exist in str(msg.message) for exist in number_words) and any(exist in str(msg.message) for exist in operation_words):
            captcha_type = "math_words"
            await bot_handler(event,captcha_type)

        if "https://t.me/" in str(msg.message):  # handle group/channel invite links if present in message
            linkdata = {}
            linkdata["msg"] = parse_message(msg)
            linkdata["client_id"] = CLIENT_ID
            linkdata["init_ch_info"] = await telegram_functions.get_init_descr(msg,client)
            future = producer.send(KafkaTopics.LINK_FEEDBACK.value, linkdata)
            result = future.get(timeout=10)  # Block until a single msg is sent
        msg_parsed = parse_message(msg)
        if msg_parsed and "channel_id" in msg_parsed:
            #Kafka producer connect
            future = producer.send(KafkaTopics.TGMESSAGES.value, msg_parsed)
            result = future.get(timeout=10)  # Block until a single msg is sent
        # Only messages in read-only channels (unlike chats) have the view count property
        if not event.is_group and not event.is_private:
            loop.create_task(telegram_functions.view_count_update(client, event, VIEW_COUNT_DELAY1_S, msg_parsed["id"], msg_parsed["channel_id"],producer))
            loop.create_task(telegram_functions.view_count_update(client, event, VIEW_COUNT_DELAY2_S, msg_parsed["id"], msg_parsed["channel_id"],producer))
            loop.create_task(telegram_functions.view_count_update(client, event, VIEW_COUNT_DELAY3_S, msg_parsed["id"], msg_parsed["channel_id"],producer))
    except Exception as e:
        logger.error(f"Exception occured: {e}", exc_info=True)


@client.on(events.ChatAction)
async def channel_events_handler(event):
    """ Processes channel event & information about channel  - triggers when client receives a new channel event
        attr - event: channel event (joined, added, left, kicked etc.)"""
    try:
        sglpr = SingletonProducer()
        producer = sglpr.get_producer()
        global event_count
        if event_count % 100 == 0:
            logger.info(f"Recorded a chat event - logging once every 100 events")
        event_count += 1
        chat_event = None
        processed_timestamp = dt.now().strftime(DATE_FORMAT)
        # Handle different events based on the presence of certain fields in the event object
        if event.new_title:
            chat_event = parse_chat_event(event, processed_timestamp, title=event.new_title)
        elif event.user_added or event.user_joined or event.user_left or event.user_kicked:
            user_id = event.user_id
            chat_event = parse_chat_event(event, processed_timestamp, user_id=user_id)
        elif event.new_pin or event.unpin:
            pinned_msg = await event.get_pinned_message()
            pinned_msg_text = pinned_msg.message if pinned_msg and hasattr(pinned_msg, "message") else None
            chat_event = parse_chat_event(event, processed_timestamp, pinned_msg=pinned_msg_text)
        if chat_event:
            #Kafka producer connect
            future = producer.send(KafkaTopics.TGCHANNEL_EVENTS.value, chat_event)
            result = future.get(timeout=10)  # Block until a single msg is sent
        await client.send_read_acknowledge(event.input_chat)
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)


async def periodic(ch_names,ch_descr,client):
    """ Executes handling tasks periodically."""
    try:
        while True:
            logger.info(f"Running a periodic check")
            client_status = 1
            client_status = await telegram_functions.checkIfClientIsRunning(client)
            sglpr = SingletonProducer()
            producer = sglpr.get_producer()
            if client_status == 1:
                open_dialogs = await telegram_functions.getOpenDialogs(client)
                feedback_data = {}
                feedback_data["client_id"] = CLIENT_ID
                feedback_data["client_status"] = client_status
                feedback_data["open_dialogs"] =[dialog.entity.id for dialog in open_dialogs]
                open_diag_status = {}
                for dialog in open_dialogs:
                    channel_id = dialog.entity.id
                    channel_link = None
                    if hasattr(dialog.entity, "participants_count"):
                        chpart_data = {}
                        chpart_data["channel_id"] = channel_id

                        chpart_data["participants_count"] = dialog.entity.participants_count
                        future = producer.send(KafkaTopics.TGCHANNEL_PARTICIPANTS_COUNT.value, chpart_data) #Kafka producer connect
                        result = future.get(timeout=10)  # Block until a single msg is sent

                feedback_data["processed_timestamp"] = time.time() #UNIX TIMESTAMP
                future = producer.send(KafkaTopics.TGCLIENT_FEEDBACK.value, feedback_data) #Kafka producer connect
                result = future.get(timeout=10)  # Block until a single msg is sent

                sglcr = SingletonConsumer() #get consumer singleton class
                consumer = sglcr.get_consumer()
                if not consumer:
                    logger.error(f"Failed to initialize a kafka consumer")
                    return
                else:
                    logger.info(consumer)

                for instructions in consumer:
                    instructions = json.loads(instructions.value)
                    channel_ids = []
                    if instructions["client_id"] == CLIENT_ID:
                        delay = time.time() - instructions["timestamp"]
                        if delay < 60: #only process the most recent instruction for current client
                            for channel_id in instructions["get_ch_descr"]:
                                await get_channel_descr(client,channel_id) #get up-to-date channel names + descríption
                            for channel_id in instructions["leave"]:
                                await leaveChannel(client, channel_id) #leave channels as instructed by manager

                            for channel_link in instructions["enter"]:
                                try:
                                    await joinChannelviaLink(client, channel_link,ch_names, ch_descr, producer)
                                    feedback_data = {}
                                    feedback_data["client_id"] = CLIENT_ID
                                    feedback_data["client_status"] = client_status
                                    feedback_data["joined_successfully"] = [channel_link]
                                    feedback_data["processed_timestamp"] = time.time() #UNIX TIMESTAMP
                                    future = producer.send(KafkaTopics.TGCLIENT_FEEDBACK.value, feedback_data) #Kafka producer connect
                                    result = future.get(timeout=10)  # Block until a single msg is sent
                                except Exception as e:
                                    continue

                            if instructions["get_ch_id"]:
                                for channel_link in instructions["get_ch_id"]:
                                    ch_ent = await client.get_entity(channel_link) #get channel ids for each channel link in list
                                    await asyncio.sleep(10) #sleep to avoid floodlimit problems
                                    ch_id = ch_ent.id
                                    channel_ids.append(ch_id)
                                channel_ids_dict = dict(zip(instructions["get_ch_id"],channel_ids)) #create dict channel_link : channel_id
                                linkdata = {}
                                linkdata["msg"] = None
                                linkdata["channel_ids"] = channel_ids_dict
                                future = producer.send(KafkaTopics.LINK_FEEDBACK.value, linkdata) #send to manager
                                result = future.get(timeout=10)  # Block until a single msg is sent

            else:
                feedback_data["client_status"] = client_status
                future = producer.send(KafkaTopics.TGCLIENT_FEEDBACK.value, feedback_data) #Kafka producer connect
                result = future.get(timeout=10)  # Block until a single msg is sent
            await asyncio.sleep(PERIODIC_CHECK_INTERVAL_S)
    except ConnectionError:
        check = 0
        wait_time = 5 #start with 5 seconds wait time
        while check == 0:
            check = start_client()
            if check != 0:
                break
            if wait_time > 43200: #43200 seconds = 12 hours
                break
            time.sleep(wait_time)
            wait_time = wait_time * 2

    except Exception as e:
        logger.error("Exception occurred", exc_info=True)


def init_channel_descr_dicts():
    """Initializes Dicts for temp storage of channel Descriptions and Names"""
    try:
        ch_names = {}
        ch_descr = {}
        return ch_names, ch_descr

    except Exception as e:
        logger.error(f"Exception in init_channel_descr_dict(): {e}", exc_info=True)


async def init_channels(ch_names, ch_descr,client,init_channel_links,producer):
    """Join initial channel list"""
    open_dialogs = await telegram_functions.getOpenDialogs(client)
    open_dialog_ids = [dialog.entity.id for dialog in open_dialogs]
    logger.info(f"Init channel links: {init_channel_links}")


    init_channel_ids = []
    for channel_link in init_channel_links:
        for attempt in range(3):
            try:
                await asyncio.sleep(45)  # The delays not to exceed the API limits, which results in long timeouts
                channel = await client.get_entity(channel_link)
                logger.info(f"Retrieved channel for link: {channel_link}") #TGCHANNEL_ALL_LINKDATA
                linkdata ={}
                linkdata["link"] = channel_link
                linkdata["channel_entity"] = channel
                linkdata["client_id"] = CLIENT_ID
                future = producer.send(KafkaTopics.TGCHANNEL_LINKS.value, linkdata)
                result = future.get(timeout=10)  # Block until a single msg is sent
                init_channel_ids.append(channel.id)
                if channel.id not in open_dialog_ids:
                    await telegram_functions.joinChannelviaLink(client, channel_link, ch_names, ch_descr,producer)
                else:
                    logger.info(f"Already joined the channel: {channel_link}, skipping...")
                break
            except Exception as e:
                logger.error(f"Failed to retrieve channel from link: {channel_link} with the following error: {e}")
                err_msg = str(e)
                if "A wait of " in err_msg:
                    timeout = re.sub("[^0-9]", "", err_msg)
                    logger.info(f"Sleeping for {timeout} seconds due to API timeout...")
                    await asyncio.sleep(int(timeout))


def get_initi_channel_links():
    try:
        with open("tg_input_channels.yaml", "r") as f:
            config = yaml.safe_load(f)
            links = config["channels"]
    except Exception as e:
        logger.error(f"Failed to read the initial channel link list with the following error: {e}")
    return links

def main():
    ch_names, ch_descr = init_channel_descr_dicts()
    logger.info(f"Checking if client {CLIENT_ID} is active.")
    init_channel_links = get_initi_channel_links()
    future = loop.run_until_complete(init_channels(ch_names, ch_descr, client,init_channel_links,producer))
    if future.done():
        task = loop.create_task(periodic(ch_names, ch_descr))
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass

        logger.info("-----Telegram Client Opened-----")
        client.run_until_disconnected()
        logger.info("-----Telegram Client Closed-----")


if __name__ == '__main__':
    main()
