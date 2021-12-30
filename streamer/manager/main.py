import json
from broker import init_kafka_consumer, init_kafka_producer, KafkaTopics
import logging
import os
import time
import pytz
from datetime import timedelta,datetime
import threading
import psycopg2
import pandas as pd
from psycopg2 import pool
from pycoingecko import CoinGeckoAPI
from parsers import parse_message, parse_chat_event, get_dialog_entity_id, DATE_FORMAT
import tracemalloc
import yaml
import json
from textblob import TextBlob

tracemalloc.start()
KAFKA_BROKER_INSTANCES = os.environ["KAFKA_BROKER_INSTANCES"].split(",")
LOG_LEVEL = os.environ["LOG_LEVEL"]
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()
if LOG_LEVEL:
    level_name = logging.getLevelName(LOG_LEVEL.upper())
    logger.setLevel(level_name)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%m"
DB_MAX_CONNECTIONS = os.environ["DB_MAX_CONNECTIONS"]
DB_CONN_POOL = None


class SingletonProducerMeta(type):
    """ Singleton metaclass """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

lock = threading.Lock()
class SingletonConsumerMeta(type):
    """ Singleton metaclass - Implemented with a lock to ensure thread safety."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(SingletonConsumerMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SingletonConsumer(metaclass=SingletonConsumerMeta):
    """ Singleton class - Implementing kafka consumer in a singleton class to ensure only one instance of consumer is open at a time."""
    def get_consumer(self,topic):
        consumer = init_kafka_consumer(KAFKA_BROKER_INSTANCES, topic)
        return consumer
    pass


class SingletonProducer(metaclass=SingletonProducerMeta):
    """ Singleton class - Implementing kafka producer in a singleton class to ensure only one instance of producer is open at a time."""
    def get_producer(self):
        producer = init_kafka_producer(KAFKA_BROKER_INSTANCES)
        return producer
    pass


def get_input_channels():
    try:
        with open("tg_input_channels.yaml", "r") as f:
            config = yaml.safe_load(f)
            channel_links = config["channels"]
    except Exception as e:
        logger.error(f"Failed to read tg input channels with the following error: {e}")
    return channel_links


def parse_links(text):
    links = re.findall(URL_REGEX, text)
    for idx, url in enumerate(links):
        temp = url.replace("joinchat/", "")
        temp = re.search("(?P<url>https?://t.me/[^/|?]+)", str(temp)).group("url")
        links[idx] = temp
    links = list(set(links))
    return links


def get_db_config():
    password = None
    host = None
    port = None
    name = None
    user = None
    try:
        with open("consumer_config.yaml", "r") as f:
            config = yaml.safe_load(f)
            password = config["db_user_password"]
            host = config["db_host"]
            port = config["db_port"]
            name = config["db_name"]
            user = config["db_user"]

    except Exception as e:
        logger.error(f"Failed to read the db password with the following error: {e}")
    finally:
        logger.debug(f"Read the following db_password: {host}, {name}, {port}, {user}, {password}")
        return host, name, port, user, password


def get_db_conn():
    global DB_CONN_POOL
    if DB_CONN_POOL is None:
        host, name, port, user, password = get_db_config()
        DB_CONN_POOL = psycopg2.pool.SimpleConnectionPool(1,DB_MAX_CONNECTIONS, user=user, password=password, host=host, port=port, database=name)
    conn = None
    while conn is None:
        try:
            conn = DB_CONN_POOL.getconn()
            yield conn
        except Exception as e:
            if "connection pool exhausted" in str(e):
                time.sleep(1)
                logger.info("Waiting for a free db conn")
            else:
                logger.error(f"Failed to get a db connection with an unexpected error: {e}")
                break
        finally:
            if conn:
                conn.commit()
                DB_CONN_POOL.putconn(conn)


def invite_link_database_checker(link):
    """ Checks if channel_id is already present in DB """
    out = None
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            # check if invitation link is in DB
            postgres_query = """SELECT EXISTS(SELECT 1 FROM telegram_link_data WHERE channel_link=%s LIMIT 1)"""
            records_to_insert = (link,)
            cur.execute(postgres_query, records_to_insert)
            out = cur.fetchone()
            out = out[0] if out else False
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error("Failed to check if an invite link is in db with the following error: {e}")
    finally:
        return out


def saved_channel_links_getter():
    """ Queries groups/channel DB table: joins groups that are free (no client_id), leaves & deletes inactive and/or
    broken groups."""
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """ SELECT channel_id,client_id,channel_link FROM telegram_link_data """
            cur.execute(postgres_query)
            df = pd.DataFrame(cur.fetchall(), columns=["channel_id","db_client_id","channel_link"])
            return df
    except Exception as e:
        logger.error(f"Failed to lookup channel_data in database with the following error: {e}")


def flag_broken_channel_db(flag_list):
    """ Flags a telegram channel as "broken" in database."""
    try:
        conn = get_db_conn().__next__()
        for broken_channel_id in flag_list:
            with conn.cursor() as cur:
                postgres_query = """UPDATE telegram_link_data SET is_broken = 1 WHERE channel_id = %s  """
                records_to_insert = (broken_channel_id,)
                cur.execute(postgres_query, records_to_insert)
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to delete channel links from db with the following error: {e}")
    return


def remove_client_id_from_db(client_id):
    """ Deletes client_id from link_data table """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """UPDATE telegram_link_data SET client_id = null WHERE client_id = %s """
            records_to_insert = (client_id,)
            cur.execute(postgres_query, records_to_insert)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to delete client number from telegram_link_data with the following error: {e}")
    return


def update_client_status_db(client_status,open_dialogs_number,client_id):
    """ update client status in database """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            if open_dialogs_number is not None:
                postgres_query = """UPDATE clients_info SET is_active = %s,nr_dialogs = %s WHERE client_id = %s """
                records_to_insert = (client_status,open_dialogs_number, client_id)
            else:
                postgres_query = """UPDATE clients_info SET is_active = %s WHERE client_id = %s """
                records_to_insert = (client_status, client_id)
            cur.execute(postgres_query, records_to_insert)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to update client status in clients_info with the following error: {e}")
    return


def get_clients_open_dialogs_db():
    """ Retrieves number of open dialogs each client currently has. Returns a dataframe """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """ SELECT client_id,is_active,nr_dialogs FROM clients_info """
            cur.execute(postgres_query)
            df = pd.DataFrame(cur.fetchall(), columns=["client_id","is_active","nr_dialogs"])
            df = df[df["is_active"] > 0] #remove inactive clients
            return df
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to update client status in clients_info with the following error: {e}")


def update_db_channel_id(channel_link,channel_id):
    """Updates client id in db"""
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """UPDATE telegram_link_data SET channel_id = %s WHERE channel_link = %s """
            records_to_insert = (channel_id,channel_link)
            cur.execute(postgres_query, records_to_insert)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to update client number in db with the following error: {e}")

    return


def update_db_client_id(channel,client_id,link_check):
    """Updates client id in db"""
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            if link_check == 0:
                postgres_query = """UPDATE telegram_link_data SET client_id = %s WHERE channel_id = %s """
            if link_check == 1:
                postgres_query = """UPDATE telegram_link_data SET client_id = %s WHERE channel_link = %s """
            records_to_insert = (client_id,channel)
            cur.execute(postgres_query, records_to_insert)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to update client number in db with the following error: {e}")
    return


def produce_to_kafka_topic(topic, value):
    sglpr = SingletonProducer()
    producer = sglpr.get_producer()
    try:
        future = producer.send(topic, value) #Kafka producer connect
        result = future.get(timeout=10)  # Block until a single msg is sent
    except Exception as e:
        logger.error(f"Failed to submit value to topic: {e}")
    return


def get_last_msg(channel_id):
    """ Retrieves last/newest message date for current channel
        attr - channel_id(int): ID of telegram channel/group"""
    query_output = None
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """SELECT last_message_date FROM telegram_link_data WHERE channel_id = %s ORDER BY last_message_date DESC LIMIT 1"""
            records_to_insert = (str(channel_id),)
            cur.execute(postgres_query, records_to_insert)
            try:
                query_output = cur.fetchall()[0][0]
            except IndexError:
                query_output = datetime.now() #If channel has been just joined and no messages have been saved to database

    except Exception as e:
        logger.error(f"Failed to get the last message data for channel_id: {channel_id} with the following error: {e}")
    finally:
        return query_output


def channelActivityChecker(channel_id):
    """ Checks if channel/group is active
        attr - channel_id(int): ID of Telegram channel/group"""
    try:
        last_message_date = get_last_msg(channel_id)
        if last_message_date and timeDeltaChecker(last_message_date):  # check if last message from channel is older than 7 days
            return 1
        else:
            return 0
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)
        return 0


def timeDeltaChecker(last_message_date):
    """ Calculates if given datetime is older than 7 days
        attr - last_message_date(datetime): Datetime of last/newest message in a TG channel/group"""
    try:
        time_delta = pytz.utc.localize(datetime.now()) - pytz.utc.localize(last_message_date)
    except Exception as e: #if timestamp is not TZ
        time_delta = datetime.now() - last_message_date
    max_time = timedelta(days=7)
    return time_delta > max_time


def channel_join_decider(link_info_string,toptickers,topnames):
    """Heuristic to decide whether to join a channel based on channel description and channel name and newest X messages from channel.
       If a name or ticker from top 50 cryptoccurrencies by mcap is present - instruct to join."""
    to_join = 0
    try:
        for name in topnames:
            if name in link_info_string:
                to_join = 1
                return to_join
        for ticker in toptickers:
            if ticker in link_info_string:
                to_join = 1
                return to_join
        return to_join

    except Exception as e:
        logger.error(f"Channel_join_decider() failed. Not entering channel. Following Error occured: {e}", exc_info=True)
        return to_join

def get_coin_update():
    """Input: N/A
       Returns: List of names and list of tickers of top 50 cryptoccurrencies from Coingecko API ordered by market cap.
       Removes popular stable coins from DataFrame."""

    try:
        cg = CoinGeckoAPI()
        coinupdate = cg.get_coins_markets(vs_currency="usd") #get all coins listed (default ordered by Mcap)
        coinupdate = coinupdate[:50]
        toptickers = []
        topnames = []
        counter = 0
        for row in coinupdate: #get tickers + names
            topnames.append(coinupdate[counter]["name"])
            toptickers.append(coinupdate[counter]["symbol"])
            counter += 1
        mystring = " "
        toptickers = [mystring + s + mystring for s in toptickers] #add space-string at start of ticker string, to avoid false positives (example: Huobi Coin (HT) is a string present in every link (HTtps://..))
        return toptickers, topnames

    except Exception as e:
        logger.error(f"Exception in get_coin_update(): {e}", exc_info=True)

def insert_manual_channel_link_list():
    """Inserts a manually curated list of telegram channel links into database"""
    channel_links = get_input_channels()
    dbch_df = saved_channel_links_getter() #get all channels + assigned clients currently saved in db
    saved_links = dbch_df["channel_link"].tolist()
    for link in channel_links:
        if link not in saved_links: #makes sure that links already saved in database, are not saved again (redundancy)
            try:
                table_name = "telegram_link_data"
                conn = get_db_conn().__next__()
                with conn.cursor() as cur:
                    language = None
                    processed_timestamp = datetime.now().strftime(DATE_FORMAT)
                    processed_timestamp_oldformat = None
                    date_oldformat = None
                    date = None
                    channel_id =None
                    channel_link = str(link)
                    message_id = None
                    posted_by_user = None
                    posted_in_channel = None
                    message_id = None
                    date_unix = None
                    date_time = None
                    posted_by_user = None
                    posted_in_channel = None
                    client_id = None
                    if date is None:  # If date is none, then link is from config set data to processed_timestamp
                        date = processed_timestamp
                    postgres_query = """INSERT INTO telegram_link_data (channel_link, channel_id, date, from_user_id, posted_in_channel_id,
                                     message_id, processed_timestamp, client_id,date_unix,lang) VALUES
                                     (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    records_to_insert = (channel_link, channel_id, date_time, posted_by_user, posted_in_channel, message_id, processed_timestamp, client_id,date_unix,language)
                    cur.execute(postgres_query, records_to_insert)
                    conn.commit()

            except (Exception, psycopg2.DatabaseError) as e:
                logger.error(f"Failed @ insert_manual_channel_link_list() with the following error: {e}")

def link_feedback_hanlder(msg):
    """ Processes a TG channel/group invitational link - triggers when client receives a TG invite link in a new message.
        Checks if tg link is already in db and initiates link storage if link is not in db
        attr - links: Telegram invitational links """
    try:
        sglpr = SingletonProducer()
        producer = sglpr.get_producer()
        toptickers, topnames = get_coin_update() #get top 50 cryptoccurrencies names and tickers from Coingecko
        if not producer:
            logger.error(f"Failed to initialize a kafka producer")
            return
        else:
            logger.info(producer)

        if msg["msg"] is not None: #if link data comes from a recorded TG msg
            link_info_dict = msg["init_ch_info"]
            links = [*link_info_dict]
        else: #if link data is channel_ids for channel_links
            channel_ids = msg["channel_ids"]
            count = 0
            for channel_link,channel_id in channel_ids.items():
                update_db_channel_id(channel_link,channel_id)
            return

        for link in links:
            link_exists = invite_link_database_checker(link) # check if channel ID is already saved to DB (check if link has already been processed)
            if not link_exists:  # if channel ID not in database, store the link
                link_info = link_info_dict[link]
                descr = link_info["descr"]
                name = link_info["name"]
                hist_messages_list = link_info["hist_messages"]
                hist_messages_string = " ".join(hist_messages_list)
                link_info_string = name + " " + descr + " " + hist_messages_string #put names, description and 50 newest messages into a single string
                link_info_string = link_info_string.lower()
                to_join = channel_join_decider(link_info_string, toptickers, topnames)

                if to_join == 1:
                    blob_lang = TextBlob(link_info_string)
                    detected_language = blob_lang.detected_language() #determine language of telegram channel with TextBlob (Google Translate API)

                    #choose appropriate client
                    clients_diags_df = get_clients_open_dialogs_db() #get open dialogs of all active clients
                    min_diags = clients_diags_df[clients_diags_df["nr_dialogs"] == pd.to_numeric(clients_diags_df["nr_dialogs"]).min()].reset_index() #get client(s) with least open dialogs
                    min_diags_client_id = min_diags["client_id"][0]

                    #send instructions to join the channel
                    if linkHandler_data["link"] is not None: #if linkdata is empty for some reason, do not forward to database
                        instructions = {}
                        instructions["client_id"] = min_diags_client_id
                        instructions["timestamp"] = time.time()
                        instructions["enter"] = [link]
                        instructions["leave"] = []
                        instructions["get_ch_descr"] = [link]
                        future = producer.send(KafkaTopics.TGINSTRUCTIONS.value, linkHandler_data)
                        result = future.get(timeout=10)  # Block until a single msg is sent
                    else:
                        logger.warning(f"@link_feedback_hanlder(): Channel link data is empty, cannot join.")

                    #send channel data + assigned client to database
                    linkHandler_data = {}
                    linkHandler_data["link"] = link
                    linkHandler_data["channel_id"] = channel_id
                    linkHandler_data["message"] = msg
                    linkHandler_data["client_id"] = min_diags_client_id
                    linkHandler_data["detected_language"] = detected_language
                    if linkHandler_data["link"] is not None: #if linkdata is empty for some reason, do not forward to database
                        future = producer.send(KafkaTopics.TGCHANNEL_LINKS.value, linkHandler_data)
                        result = future.get(timeout=10)  # Block until a single msg is sent
                    else:
                        logger.warning(f"@link_feedback_hanlder(): Channel link data is empty, cannot store link.")
                else:
                    logger.info(f"@link_feedback_hanlder(): Channel {link} deemed to be irrelevant. Not joining, moving on.")
            else:
                logger.info(f"@link_feedback_hanlder(): Channel link {link} already stored, moving on.")
        return

    except Exception as e:
        logger.error(f"Failed to process the links: {links} from the original messages: {msg.message}"
                     f" with the following error: {e}")


def client_feedback_handler(feedback):
    feedback_json = feedback
    time_delta = time.time() - feedback_json["processed_timestamp"] #unix
    max_time = 35 * 60
    #Client Feedback will only be processed if it is not older than 35 minutes
    #Clients perform periodic check every 30 minutes - this way we only process the latest feedback data
    #and don't process things that were already processed (on restarts)
    if not time_delta > max_time:
        client_id = feedback_json["client_id"]
        client_status = feedback_json["client_status"]

        check = feedback_json.get("joined_successfully")
        if check is not None: #Update client_id of free channels that have been recently joined by a client
            joined_channels = feedback_json["joined_successfully"]
            link_check = 1
            for channel in joined_channels:
                update_db_client_id(channel,client_id,link_check)
            return

        if client_status == 1: #if client is running
            open_dialogs = feedback_json["open_dialogs"] #get open dialogs
            broken_channels = []
            get_ch_descr = []
            for channel_id in open_dialogs:
                is_inactive = channelActivityChecker(channel_id)
                if is_inactive == 1:
                    broken_channels.append(key)
                else:
                    get_ch_descr.append(channel_id)
            if broken_channels: #if inactive channels exist
                flag_broken_channel_db(broken_channels) #flag instead of removing + date of flag
            update_client_status_db(client_status,len(open_dialogs),client_id) #update client status

        if client_status == 0:
            remove_client_id_from_db(client_id) #remove client_id from link_data table
            open_dialogs = None
            update_client_status_db(client_status, open_dialogs, client_id) #update client status in database as broken

        client_channels = [channel_id for channel_id in open_dialogs if channel_id not in broken_channels] #remove inactive channels from list of client open channels
        instruct_to_leave = []
        saved_channels_no_id = []
        disregard = []
        dbch_df = saved_channel_links_getter() #get all channels + assigned clients currently saved in db
        for i in range(0, len(dbch_df.index)):
            if dbch_df.iloc[i]["channel_id"] is None:
                saved_channels_no_id.append(dbch_df.iloc[i]["channel_link"]) #get channels that are saved in database, but have no channel_id
            else:
                if int(dbch_df.iloc[i]["channel_id"]) in client_channels:
                    if dbch_df.iloc[i]["db_client_id"] != client_id:
                        if dbch_df.iloc[i]["db_client_id"] is None:
                            link_check = 0
                            update_db_client_id(dbch_df.iloc[i]["channel_id"],client_id,link_check) #if active channel is already in db, but client_id not saved - update client_id
                            disregard.append(dbch_df.iloc[i]["channel_id"])
                        if dbch_df.iloc[i]["db_client_id"] is not None:
                            instruct_to_leave.append(dbch_df.iloc[i]["channel_id"])

        db_channels_no_client = []
        for index,row in dbch_df.iterrows(): #get db-channels that have no client
            if not isinstance(row["db_client_id"], int):
                db_channels_no_client.append(row["channel_link"])
        to_join_links = [x for x in db_channels_no_client if x not in disregard] #remove channels where assigned client has been update

        clients_diags_df = get_clients_open_dialogs_db() #get open dialogs of all active clients
        min_diags = clients_diags_df[clients_diags_df["nr_dialogs"] == pd.to_numeric(clients_diags_df["nr_dialogs"]).min()] #get client(s) with least open dialogs
        min_diags_client_id = min_diags["client_id"].tolist()
        if client_id in min_diags_client_id: #if current client does not have the least open dialogs, prevent client from entering new channels

            space_left = 500 - len(client_channels) + len(broken_channels)
            if len(to_join_links) > space_left: #If more non-assigned channels in db than space left
                instruct_to_enter = to_join_links[:(space_left-3)] #take as many as possible and instruct client to enter ( safety margin of 3 channels in case client enters new channels while intructions being sent)
            else:
                instruct_to_enter = to_join_links
        else:
            instruct_to_enter = []

        instructions = {}
        instructions["client_id"] = client_id
        instructions["timestamp"] = time.time()
        instructions["enter"] = instruct_to_enter
        instructions["leave"] = instruct_to_leave
        instructions["get_ch_descr"] = get_ch_descr
        instructions["get_ch_id"] = saved_channels_no_id
        produce_to_kafka_topic(KafkaTopics.TGINSTRUCTIONS.value, instructions)

def consume_msgs(topic):
    sglcr = SingletonConsumer()
    consumer = sglcr.get_consumer(topic)
    if not consumer:
        logger.error(f"Failed to initialize a kafka consumer")
        return
    else:
        logger.info(consumer)

    for msg in consumer:
        try:
            msg_json = json.loads(msg.value)
            if topic == KafkaTopics.TGCLIENT_FEEDBACK.value:

                logger.info(f"Retrieved a client feedback from kafka: {msg_json}")
                client_feedback_handler(msg_json)

            if topic == KafkaTopics.LINK_FEEDBACK.value:
                msg_json = json.loads(msg.value)
                logger.info(f"Retrieved a link feedback from kafka: {msg_json}")
                link_feedback_hanlder(msg_json)

        except Exception as e:
            logger.error(f"Failed to store a msg from topic {topic}, with the following error: {e}")


def main():
    logger.info("Saving Telegram channels list from external file to database")
    insert_manual_channel_link_list()
    logger.info("Telegram channels list from external file has been saved to the database.")
    feedback_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.TGCLIENT_FEEDBACK.value]))
    feedback_thread.start() #client feedback
    link_data_thread = threading.Thread(target=consume_msgs, args=([KafkaTopics.LINK_FEEDBACK.value]))
    link_data_thread.start() #link data


if __name__ == '__main__':
    main()
