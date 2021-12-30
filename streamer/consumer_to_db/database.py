import psycopg2
import re
from datetime import datetime

#import telegram_functions
from parsers import get_dialog_entity_id
import logging
from psycopg2 import pool
import time
import os
import yaml
import json
import time
from datetime import datetime as dt
DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%m"
DB_MAX_CONNECTIONS = os.environ["DB_MAX_CONNECTIONS"]
DB_CONN_POOL = None
logger = logging.getLogger()


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


def insert_message(msg):
    """ Saves telegram message + meta-information to the PostgreSQL database server """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:

            postgres_query = """INSERT INTO telegram_messages (id, channel_id, date, text, mentioned, media_unread,
                silent, post, from_scheduled, legacy, edit_hide, fwd_from, via_bot_id, reply_to_msg_id,
                views, edit_date, grouped_id, restriction_reason,
                processed_timestamp, out, from_id, view_count_5min, view_count_20min, view_count_60min,date_unix) VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            date_time = datetime.strptime(msg["date"], "%Y-%m-%d %H:%M:%S.%f")
            date_unix = date_time.timestamp()
            records_to_insert = (msg["id"], msg["channel_id"], date_time, msg["text"], msg["mentioned"],
                                 msg["media_unread"], msg["silent"], msg["post"], msg["from_scheduled"], msg["legacy"],
                                 msg["edit_hide"], msg["fwd_from"], msg["via_bot_id"], msg["reply_to_msg_id"],
                                 msg["views"], msg["edit_date"], msg["grouped_id"], msg["restriction_reason"],
                                 msg["processed_timestamp"], msg["out"], msg["from_id"], msg["view_count_5min"],
                                 msg["view_count_20min"], msg["view_count_60min"],date_unix)
            cur.execute(postgres_query, records_to_insert)
            conn.commit()

            update_last_message_date(date_time, msg["channel_id"]) #update the date/time of newest message for the channel of message

    except (Exception, psycopg2.DatabaseError) as e:
        logger.info(f"Failed to insert a TG message to database with the following error {e}")


def insert_link_data(message, msg=None, channel_entity=None, from_config=False):
    """ Saves new telegram channel id, invitation links & meta info to the PostgreSQL database server """
    try:
        table_name = "telegram_link_data"

        check = message.get("client_id")
        if check is not None:
            client_id = message["client_id"]
        else:
            client_id = None
        check = message.get("channel_id")
        if check is not None:
            channel_id = message["channel_id"]
        else:
            channel_id = None
        msg = message["message"]
        link = message["link"]
        language = message["detected_language"]
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            processed_timestamp = dt.now().strftime(DATE_FORMAT)
            processed_timestamp_oldformat = None
            date_oldformat = None
            date = None
            channel_id = channel_id #str(channel_entity.id) if channel_entity else None
            channel_link = str(link)
            message_id = None
            posted_by_user = None
            posted_in_channel = None
            if msg:
                message_id = str(msg.id) if hasattr(msg, "id") else None
                date_unix = time.mktime(msg.date.timetuple()) if hasattr(msg, "date") else None
                date_time = datetime.strptime(msg["date"], "%Y-%m-%d %H:%M:%S.%f") if hasattr(msg, "date") else None
                posted_by_user = str(msg.from_id.user_id) if hasattr(msg, "from_id") and hasattr(msg.from_id, "user_id") else None
                posted_in_channel = re.sub("[^0-9]", "", str(msg.to_id)) if hasattr(msg, "to_id") else "Null"
            if date is None:  # If date is none, then link is from config set data to processed_timestamp
                date = processed_timestamp
            postgres_query = """INSERT INTO telegram_link_data (channel_link, channel_id, date, from_user_id, posted_in_channel_id,
                             message_id, processed_timestamp, client_id,date_unix,lang) VALUES
                             (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            records_to_insert = (channel_link, channel_id, date_time, posted_by_user, posted_in_channel, message_id, processed_timestamp, client_id,date_unix,language)
            cur.execute(postgres_query, records_to_insert)
            conn.commit()

    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to insert telegram link_data into {table_name} with the following error: {e}")


def format_to_sql(value):
    return "\'" + value + "\'" if value else "NULL"


def invite_link_database_checker(link):
    """ Checks if channel_id is already present in DB """
    out = None
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            # check if invitation link is in DB
            postgres_query = """SELECT EXISTS(SELECT 1 FROM telegram_collected_link_data WHERE channel_link=%s LIMIT 1)"""
            records_to_insert = (link,)
            cur.execute(postgres_query, records_to_insert)
            out = cur.fetchone()
            out = out[0] if out else False
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error("Failed to check if an invite link is in db with the following error: {e}")
    finally:
        return out


def update_last_message_date(last_message_date, channel_id):
    """ Updates last/newest message date for current channel
        attr - channel_id(int): ID of telegram channel/group
        attr - last_message_date(str): Datetime of latest message in channel/group"""
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """UPDATE telegram_link_data SET last_message_date=%s WHERE channel_id=%s """
            records_to_insert = (last_message_date, str(channel_id))
            cur.execute(postgres_query, records_to_insert)
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)


def insert_chat_event(chat_event):
    """ Saves telegram channel events (joined,left,kicked etc.) to the PostgreSQL database server """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            date_unix = time.mktime(msg.date.timetuple()) if hasattr(chat_event, "date") else None
            postgres_query = """INSERT INTO telegram_chat_events (id, date, title_event, pin_msg_event, unpin_msg_event,
                user_event, title, pinned_msg, user_id, chat_id, processed_timestamp,date_unix) VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            records_to_insert = (chat_event["id"], chat_event["date"], chat_event["title_event"],
                                 chat_event["pin_msg_event"], chat_event["unpin_msg_event"], chat_event["user_event"],
                                 chat_event["title"], chat_event["pinned_msg"], chat_event["user_id"],
                                 chat_event["chat_id"], chat_event["processed_timestamp"],date_unix)
            cur.execute(postgres_query, records_to_insert)
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to insert chat_event to database with the following error: {e}")

def insert_client_info(client_info):
    """ Saves client information to the PostgreSQL database server """
    try:
        conn = get_db_conn().__next__()
        processed_timestamp_unix = int(time.time()) #unix timestamp
        with conn.cursor() as cur:
            postgres_query = """SELECT * FROM clients_info """
            cur.execute(postgres_query)
            df = pd.DataFrame(cur.fetchall(), columns=["client_id","phone_number","is_active","nr_dialogs","processed_timestamp_unix"])
            stored_ids = df["client_id"].tolist()
            current_id = client_info["client_id"]
            if current_id not in stored_ids:
                postgres_query = """INSERT INTO clients_info (client_id,phone_number,is_active,nr_dialogs,processed_timestamp_unix) VALUES
                    (%s, %s, %s, %s, %s) """
                records_to_insert = (client_info["client_id"],client_info["phone_number"],client_info["is_active"],client_info["open_dialogs"], processed_timestamp_unix)
                cur.execute(postgres_query, records_to_insert)
                conn.commit()
            else:
                logger.info(f"Client ID {current_id} already stored in clients_info table. Moving on.")
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to insert client_info to database with the following error: {e}")

def delete_client_id(client_id):
    """ Deletes client_id from DB """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """UPDATE telegram_link_data SET client_id = null WHERE client_id = %s """
            records_to_insert = (client_id,)
            cur.execute(postgres_query, records_to_insert)
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to delete client number from db with the following error: {e}")


def check_if_channel_valid_for_client(channel_id, client_id):
    """ Checks if given dialog(channel or chat) exists in db and its assigned client_id. A dialog is valid for a
    provided client_id if it the dialog exists in the db and client_id column is NULL or equal to the provided client_id."""
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT EXISTS(SELECT client_id FROM telegram_link_data WHERE channel_id = '{channel_id}' LIMIT 1)"
            )
            out = cur.fetchall()
            if not out[0][0]:  # If False, then channel not in db
                return False
            cur.execute(f"SELECT client_id FROM telegram_link_data WHERE channel_id='{channel_id}' LIMIT 1")
            out = cur.fetchall()
            return out[0][0] == 'None' or out[0][0] == client_id
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to lookup channel_id in database with the following error: {e}")


def check_if_channel_present_and_has_client(channel_id):
    """ Check if channel_id is stored in the table telegram_link_data. If so, checks which client id is assigned to the
    channel. Returns results of the queries: exists (bool) and client_id (int)."""
    exists = False
    client_id = None
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            cur.execute(f"SELECT EXISTS(SELECT channel_link FROM telegram_link_data where channel_id = '{channel_id}')")
            out = cur.fetchone()
            exists = out[0] if out else False
            cur.execute(f"SELECT client_id FROM telegram_link_data where channel_id = '{channel_id}' LIMIT 1")
            out = cur.fetchone()
            client_id = out[0] if out else None
    except Exception as e:
        logger.error(f"Failed to verify whether a channel is present in db with the following error: {e}")
    finally:
        return exists, client_id


async def check_if_channel_data_in_database(client, client_id,ch_names, ch_descr):
    """ Queries groups/channel DB table: joins groups that are free (no client_id), leaves & deletes inactive and/or
    broken groups."""
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """ SELECT channel_id,client_id,channel_link FROM telegram_link_data """
            cur.execute(postgres_query)
            query_output = cur.fetchall()
            for row in query_output:
                row = str(row)
                channel_id = row.split(",")[0]
                channel_id = re.sub('[^0-9]+', '', channel_id)
                db_client_id = row.split(",")[1]
                db_client_id = re.sub('[^0-9]+', '', db_client_id)
                db_client_id = db_client_id if db_client_id else None
                channel_link = row.split(",")[2]
                channel_link = channel_link[2:-2]
                # If channel/group has no client_id, try joining
                result = await telegram_functions.channelActivityChecker(client, db_client_id, client_id,
                                                                         channel_id, channel_link,ch_names, ch_descr) #ch_names, ch_descr
                if result == 1:  # If channel/group is inactive or otherwise broken: leave channel & delete channel from DB
                    await telegram_functions.leaveChannel(client, channel_id)
                    delete_channel_from_database(channel_id)
    except Exception as e:
        logger.error(f"Failed to lookup channel_data in database with the following error: {e}")


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
            query_output = cur.fetchall()[0][0]
    except Exception as e:
        logger.error(f"Failed to get the last message data for channel_id: {channel_id} with the following error: {e}")
    finally:
        return query_output


def update_client_id(client_id, channel_id):
    """ Assigns client number to channel_id in DB """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """UPDATE telegram_link_data SET client_id = %s WHERE channel_id = %s """
            records_to_insert = (client_id, str(channel_id))
            cur.execute(postgres_query, records_to_insert)
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to update the client_id - channel_id association in database with the following "
                      f"error: {e}")


def delete_channel_from_database(channel_id):
    """ Deletes channel from DB given channel ID """
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            postgres_query = """DELETE FROM telegram_link_data WHERE channel_id = %s """
            records_to_insert = (str(channel_id),)
            cur.execute(postgres_query, records_to_insert)
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to delete a channel from database with the following error: {e}")


def update_view_count(viewupdate):
    """ Updates view count for message after 5,20,60min """
    try:
        msg_id = viewupdate["msg_id_db"]
        channel_id = viewupdate["channel_id"]
        delay = viewupdate["delay"]
        view_count = viewupdate["view_count"]
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            query = ""
            if delay == 300:
                query = "UPDATE telegram_messages SET view_count_5min = %s WHERE channel_id = %s and id = %s"
            elif delay == 1200:
                query = "UPDATE telegram_messages SET view_count_20min = %s WHERE channel_id = %s and id = %s"
            elif delay == 3600:
                query = "UPDATE telegram_messages SET view_count_60min = %s WHERE channel_id = %s and id = %s"
            records_to_update = (view_count, str(channel_id), str(msg_id))
            cur.execute(query, records_to_update)
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed to update message view count in database with the following error: {e}")


def delete_all_channel_links():
    try:
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM telegram_link_data;")
    except Exception as e:
        logger.error(f"Failed to remove channel_links from telegram_link_data, with the following error: {e}")


def insert_channel_data(chpart_data):
    try:
        channe_id = chpart_data["channel_id"]
        participants_count = chpart_data["participants_count"]
        processed_timestamp = time.time() #unix timestamp
        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO telegram_channel_data (channel_id, participant_count, processed_timestamp,processed_timestamp_unix,is_broken) values"
                        f" ('{channel_id}', '{participant_count}', None, '{processed_timestamp}');")
    except Exception as e:
        logger.error(f"Failed to insert channel_data into telegram_channel_data, with the following error: {e}")


def insert_channel_descr(channel_descr_updater_data):
    try:
        ch_info = channel_descr_updater_data["ch_info"]
        check = channel_descr_updater_data["check"]
        ch_name = ch_info["name"]
        ch_descr = ch_info["descr"]
        channel_id = ch_info["channel_id"]

        conn = get_db_conn().__next__()
        with conn.cursor() as cur:
            if check == 1:
                cur.execute(f"INSERT INTO telegram_channel_info (channel_name, channel_descr, channel_id) values"
                            f" ('{ch_name}', '{ch_descr}','{channel_id}');")
            if check != 1:
                query = ""
                if check == 2:
                    query = "UPDATE telegram_channel_info SET channel_name = %s WHERE channel_id = %s"
                    records_to_update(str(ch_name),str(channel_id))
                if check == 3:
                    query = "UPDATE telegram_channel_info SET channel_descr = %s WHERE channel_id = %s"
                    records_to_update(str(ch_name),str(channel_id))
                cur.execute(query, records_to_update)

    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Failed at insert_channel_descr() with the following error: {e}")
