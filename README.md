# Telegram_Streamer
This is a scalable system that reads messages from public Telegram channels using Telethon and stores the data in a PostgreSQL database.  Its original intention is to monitor cryptocurrency related channels, but it can be configured to read any Telegram data that is accessible through the API. The manager is the control unit of this infrastructure as it directs the actions of the producers and evaluates their status. The producers only collect Telegram data in the way the manager instructs them to. The consumer is responsible for storing data in a Postgresql database.

**Producer directory:** consists of a broker script (Kafka integration), a main script and a telegram_functions script

*producer/main.py:*
Initializes the Telegram Client. Listens to messages, chat events (user joined, kicked, left etc). If a telegram
channel link is found in a message, the link and channel name / description is forwarded to the manager.
Executes a periodic function at defined intervals where the following actions are performed: check whether
the client is active, check whether the channels the client is part of are active, send a feedback (client active?
Channels active?) to the manager, receive feedback from the manager (new channels to join, channels to
leave), execute instructions of manager.

*producer/telegram_functions:*
Helper functions for producer/main.py - mainly Telegram API calls: get channel description, solve anti-bot
captcha, join channel, leave channel

**Consumer directory:** consists of a broker script (Kafka integration), a main script and a database script.

*consumer_raw_store/main.py:*
Listens to Kafka topics and forwards Kafka messages to the appropriate database functions.

*consumer_raw_store/database.py:*
Collection of database functions for interacting with Postgresql: save messages, save links, query database,
update fields, delete information from database

**Manager directory:** consists of a broker script (Kafka integration) and a main script

*manager/main.py:*
Handles client feedback: processes whether client is active, processes whether client has inactive channels.
Updates database with new information from client feedback. Handles link feedback: processes whether
channel link is already stored in database, processes whether channel link is relevant in the context of
cryptoassets. Updates database with new information from link feedback. Sends intrudctions to producer(s):
which channels to join (via link), which channels to leave, for which channels to collect additional information
(latest channel description + name).


[work in progress, database specifications will be added shortly]
