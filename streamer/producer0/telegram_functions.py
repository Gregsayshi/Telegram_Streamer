import telethon
from datetime import timedelta
from datetime import datetime as dt
from telethon.tl import functions
from broker import *
from parsers import *
import pytz
import re
import logging
import asyncio
from telethon.tl.functions.channels import GetFullChannelRequest


logger = logging.getLogger()
URL_REGEX = "https?://t.me/(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"

def parse_links(text):
    links = re.findall(URL_REGEX, text)
    for idx, url in enumerate(links):
        temp = url.replace("joinchat/", "")
        temp = re.search("(?P<url>https?://t.me/[^/|?]+)", str(temp)).group("url")
        links[idx] = temp
    links = list(set(links))
    return links

async def get_init_descr(msg,client):
    """Enters a channel, gets description and name of channel, then leaves.
       Returns channel description and channel name. """

    links = parse_links(msg.message)
    myself = await client.get_me()
    open_dialogs = await client.get_dialogs(myself.id)
    open_dialog_ids = [x.entity.id for x in open_dialogs]
    init_ch_info = {}
    for link in links:
        parsed_msgs = []
        try:
            entity = await client.get_entity(link)
            if entity.id not in open_dialog_ids: #dont join channels where client is already member
                await client(functions.channels.JoinChannelRequest(link)) #join channel
                logger.info(f"Successfully joined: {link}")
                async for msg in client.iter_messages(entity.id):
                    parsed_msg = parse_message(msg)
                    if parsed_msg:
                        parsed_msgs.append(parsed_msg["text"])
                    if len(parsed_msgs) >= 50: # Stop after collecting 50 messages
                        logger.info(f"{len(parsed_msgs)} messages for channel {link} collected - stopping collection.")
                        break
                ch_info = await get_channel_descr(client, link) #get descr
                ch_info["link"] = link
                ch_info["hist_messages"] = parsed_msgs
                init_ch_info[link] = ch_info
                #leave channel
                await leaveChannel(client, entity.id)
                logger.info(f"Successfully left: {link}")
                await asyncio.sleep(10) #sleep to avoid floodlimit problems
        except Exception as e:
            logger.error(f"Failed at get_init_descr() with the following error: {e}")

    return init_ch_info


def channel_descr_updater(channel_link, ch_info, ch_names, ch_descr, producer):
    """ Saves Channel Name and Description in a Dictionary. Calls Database Updater if new channel descr != saved channel descr.
        attr - channel_link: Telegram invite link, ch_info: Channel Descr and Name"""
    check = 0
    try:
        chname_new = ch_info["name"]
        chdescr_new = ch_info["descr"]
        #If no key for channel exists, create new key
        if channel_link not in ch_names:
            ch_names[channel_link] = chname_new
            check = 1
        else:
            if chname_new != ch_names[channel_link]:
                check = 2

        if channel_link not in ch_descr:
            ch_descr[channel_link] = chdescr_new
            check = 1
        else:
            if chdescr_new != ch_descr[channel_link]:
                check = 3

        if check != 0:
            #Kafka producer connect
            channel_descr_updater_data = {}
            channel_descr_updater_data["link"] = channel_link
            channel_descr_updater_data["ch_info"] = ch_info
            channel_descr_updater_data["check"] = check
            future = producer.send(KafkaTopics.TGCHANNEL_INFO.value, channel_descr_updater_data)
            result = future.get(timeout=10)  # Block until a single msg is sent

        return

    except Exception as e:
        logger.error(f"Failed at channel_descr_updater() with the following error: {e}")

async def joinChannelviaLink(client, link,ch_names, ch_descr, producer): #ch_names, ch_descr
    """ Joins Telegram channel via invitational Link
        attr - client: Telegram client object
        attr - link(str): Invitational link for TG channel/group"""
    try:
        channel_link = link
        await client(functions.channels.JoinChannelRequest(link))
        logger.info(f"Successfully joined: {link}")

        ch_info = await get_channel_descr(client, channel_link)
        channel_descr_updater(channel_link, ch_info, ch_names, ch_descr,producer)

        #assigned_coin = get_channel_coin(client, channel_link, ch_info) # TODO INGEGRATE WITH CONSUMER_TRANSFORM - MOVE TO CONSUMER_TRANSFORM!!!
        #IMPLEMENT STORAGE OF COIN TO-DO!

    except Exception as e:
        logger.error(f"Failed to join a channel via link: {link} with the following error: {e}")


async def checkIfClientIsRunning(client):
    """ Checks if current client (phone number) is banned or otherwise disabled
        attr - client: Telegram client object"""
    try:
        myself = await client.get_me()
        if myself:
            return 1
        else:
            return 0
    except telethon.errors.rpcbaseerrors.BadRequestError as e:
        logger.error(f"Failed to checkIfClientIsRunning: Bad Request Error - POSSIBLY BANNED!: {e}")
        return 0
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)
        return 0


async def getOpenDialogs(client):
    """ Retrieves channels, groups & conversations the account/client is currently part of
        attr - client: Telegram client object"""
    try:
        myself = await client.get_me()
        open_dialogs = await client.get_dialogs(myself.id)
        return open_dialogs
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)


async def leaveChannel(client, channel_id):
    """ Leaves TG group/channel with given channel_id and given client
        attr - client: Telegram client object
        attr - channel_id(int): ID of Telegram channel/group"""
    try:
        await client.delete_dialog(int(channel_id))
        logger.info(f"Successfully left channel: {channel_id}")
        return 1
    except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
        try:
            client(functions.channels.DeleteHistoryRequest(channel_id,max_id=0)) #try to delete
        except Exception as e:
            logger.error("Exception occurred while trying to delete Dialog @ DeleteHistoryRequest", exc_info=True)
            pass
        return 0
    except telethon.errors.UserNotParticipantError as e:
        return 0
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)
        return 0

async def view_count_update(client, event, delay, msg_id_db, channel_id, producer):
    await asyncio.sleep(delay)
    out = await client(functions.channels.GetMessagesRequest(channel=event.message.to_id, id=[event.message.id]))
    view_count = out.messages[0].views
    viewupdate = {}
    viewupdate["msg_id_db"] = msg_id_db
    viewupdate["channel_id"] = channel_id
    viewupdate["delay"] = delay
    viewupdate["view_count"] = view_count

    future = producer.send(KafkaTopics.VIEWCOUNT_UPDATE.value, viewupdate)
    result = future.get(timeout=10)  # Block until a single msg is sent

    return

async def get_channel_descr(client,channel_id):
    """ Queries Telethon API for channel name and description based on channel link.
        attr - channel_link: Link to telegram channel"""
    try:
        ch_ent = await client.get_entity(channel_id)
        await asyncio.sleep(10)
        ch_title = ch_ent.title #get title
        try:
            ch_full = await client(GetFullChannelRequest(channel_id))
            ch_descr = ch_full.full_chat.about #get description
        except Exception as e:
            logger.error(f"Failed at GetFullChannelRequest(channel_id) with the following error: {e}")
            ch_descr = None
            pass

        ch_info = {}
        ch_info["name"] = ch_title
        ch_info["descr"] = ch_descr
        ch_info["channel_id"] = ch_ent.id
        return ch_info

    except Exception as e:
        logger.error(f"Failed at get_entity(channel_id) with the following error: {e}")
        return None
