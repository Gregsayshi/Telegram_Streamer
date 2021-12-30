import datetime
import uuid
import re
from datetime import datetime
import logging
from telethon.tl.types import MessageActionPinMessage

DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%m"
logger = logging.getLogger()


def convert_date_to_utc(date_field, field_date_format):
    date = datetime.strptime(date_field, field_date_format)
    return date.strftime("%Y-%m-%d %H:%M:%S.%m")


def createUniqueId():
    """ Creates unique ID """
    try:
        id = uuid.uuid4().hex
        return id
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)


def parse_message(response_msg):
    msg = None
    try:
        # create unique ID for message because Telegram only provides unique ids in the context of a single channel
        from_id = None
        if hasattr(response_msg, "from_id"):
            if hasattr(response_msg.from_id, "channel_id"):
                from_id = response_msg.from_id.channel_id
            elif hasattr(response_msg.from_id, "user_id"):
                from_id = response_msg.from_id.user_id
        channel_id = re.sub("[^0-9]", "", str(response_msg.to_id))
        msg = {
            "id": response_msg.id,
            "channel_id": channel_id,
            "date": response_msg.date.strftime(DATE_FORMAT),
            "text": response_msg.message,
            "out": response_msg.out,
            "mentioned": response_msg.mentioned,
            "media_unread": response_msg.media_unread,
            "silent": response_msg.silent,
            "post": response_msg.post,
            "from_scheduled": response_msg.from_scheduled,
            "legacy": response_msg.legacy,
            "edit_hide": response_msg.edit_hide,
            "from_id": from_id,
            "fwd_from": str(response_msg.fwd_from),
            "via_bot_id": response_msg.via_bot_id,
            "reply_to_msg_id": response_msg.reply_to_msg_id,
            "views": response_msg.views,
            "edit_date": response_msg.edit_date,
            "grouped_id": response_msg.grouped_id,
            "restriction_reason": str(response_msg.restriction_reason),
            "view_count_5min": None,
            "view_count_20min": None,
            "view_count_60min": None,
            "processed_timestamp": datetime.now().strftime(DATE_FORMAT)
        }
    except Exception as e:
        logger.error(f"Failed to parse a telegram message with the following error: {e}")
    finally:
        return msg


def parse_chat_event(event, processed_timestamp, user_id=None, title=None, pinned_msg=None):
    chat_event = None
    try:
        user_event = event.user_added or event.user_joined or event.user_left or event.user_kicked
        title_event = bool(event.new_title)
        pin_msg_event = False
        if hasattr(event.action_message, "action"):
            pin_msg_event = type(event.action_message.action) == MessageActionPinMessage
        chat_event = {
            "id": createUniqueId(),
            "date": event.action_message.date.strftime(DATE_FORMAT) if event.action_message else processed_timestamp,
            "user_event": user_event,
            "title_event": title_event,
            "pin_msg_event": pin_msg_event,
            "unpin_msg_event": event.unpin and not pin_msg_event and not user_event and not title_event,
            "user_id": user_id,
            "title": title,
            "pinned_msg": pinned_msg,
            "chat_id": event.chat_id,
            "processed_timestamp": processed_timestamp
        }
    except Exception as e:
        logger.error(f"Failed to parse telegram chat event with the following error: {e}")
    finally:
        return chat_event


def get_dialog_entity_id(dialog):
    peer_id = None
    if hasattr(dialog.input_entity, "channel_id"):
        peer_id = dialog.input_entity.channel_id
    elif hasattr(dialog.input_entity, "chat_id"):
        peer_id = dialog.input_entity.chat_id
    elif hasattr(dialog.input_entity, "user_id"):
        peer_id = dialog.input_entity.user_id

    return peer_id
