import re
import time
import logging
import operator
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from collections import Counter
from pycoingecko import CoinGeckoAPI
from expiringdict import ExpiringDict


start = datetime.now()
logger = logging.getLogger()
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

        disregardnames = ["Tether","USD Coin","Binance USD","cDAI","cUSDC","Dai"]
        disregardtickers = ["usdt","usdc","busd","dai","cusdc","cdai"]
        toptickers = [x.lower() for x in toptickers if x not in disregardtickers] #remove stable-coins from list and force lowercase
        mystring = " "
        toptickers = [mystring + s + mystring for s in toptickers] #add space-string at start of ticker string, to avoid false positives (example: Huobi Coin (HT) is a string present in every link (HTtps://..))
        topnames = [x.lower() for x in topnames if x not in disregardnames] #remove stable-coins from list

        return toptickers, topnames

    except Exception as e:
        logger.error(f"Exception in get_coin_update(): {e}", exc_info=True)


def init_dicts():
    """Initializes ExpiringDict structures for temp storage of message data
       max_len = Maximum number of keys in Dict, max_age_seconds= Maximum age of keys in Dict
       If value > max: key is deleted from dict """
    try:
        #Optimal TTL would be 30720*60 (8 days) or 15360*60 (4 days), but that exceeds the hardware limitations
        #With TTL 60min, we get ~13% attribution rate instead of ~34%
        redd = ExpiringDict(max_len=100000,max_age_seconds=((60*60)))

        twitd = ExpiringDict(max_len=100000,max_age_seconds=((120*60))) #Optimal TTL is 120min

        teled = ExpiringDict(max_len=100000,max_age_seconds=((120*60))) #Optimal TTL is 120min
        return redd,twitd,teled

    except Exception as e:
        logger.error(f"Exception in init_dicts(): {e}", exc_info=True)

def coin_to_text_assigner(data,belongs_to,redd,twitd,teled,toptickers,topnames):
    """Input: "data" - text & metadata (JSON), "belongs_to" - data source identifier,
       "redd","twitd","teled" - temporary msg storage in form dicts, "toptickers","topnames" - top 50 cryptocurrencies by Mcap

       Returns: Assigned coin to current message as string or returns NaN."""

    try:
        #Extract relevant information from JSON
        """ for Twitter API & Reddit API integration

        if "Twitter" in belongs_to:
            text = data["text"]
            msgid = float(data["id"]) #float because parent ID may be imported as a float due to existence of NaNs
            parent_id = data["reply_to_tweet_id"]
            timestamp = data["created_at"]

        if "Reddit" in belongs_to:
            text = data["text"]#data["text"]
            try:
                text = data["title"] + " " + data["text"] #if message is original post, get also the title
            except KeyError: #continue if not
                pass
            msgid = data["id"]
            try:
                parent_id = data["submission_id"]
            except KeyError:
                parent_id = np.nan #if reddit message is original post, it has no parent_id field
            timestamp = data["created_at"]
        """

        if "Telegram" in belongs_to:
            text = data["text"]
            msgid = data["id"] #float because parent ID may be imported as a float due to existence of NaNs
            parent_id = data["reply_to_msg_id"]
            timestamp = data["date"]


        def assigner(l,text):
            """Input: list of target items, text
               Returns: list of items found in text that are part of list of target items (toptickers or topnames).
               -> Disregards strings /BTC,/btc to exclude irrelevant price updates (price updates in the form of ALT/BTC)."""
            try:
                itemlist = []
                black_list = ["/BTC","/btc","etc"," ht","amp"," ht "," etc ","tron","dot","algo","link"," link ", " dot ", " maker ", " tron ", " amp "]#cant handle ETC,HT string - too many unrelated occurences
                for item in l:
                    if item in str(text):
                        if str(item) not in black_list:
                            itemlist.append(item)
                return itemlist

            except Exception as e:
                logger.error(f"Exception in assigner(): {e}", exc_info=True)

        def to_names(toptickers,topnames, assignedtickers, assignednames):
            """Input: toptickers,topnames - tickers/names of top 50 cryptoccurrencies,
               assingedtickers - top50 tickers found in message, assignednames - top50 names found in message
               Translates top50 tickers into full names.
               Returns: assignednames """
            try:
                topcryptodict = dict(zip(toptickers,topnames))
                namesfromtickers = []
                for ticker in assignedtickers:
                    if "$" in ticker:
                        ticker = ticker.replace("$", "")
                        ticker = " "+ticker+" "
                    if "#" in ticker:
                        ticker = ticker.replace("#","")
                        ticker = " "+ticker+" "
                    namesfromtickers.append(topcryptodict[ticker]) #if tickers are present, get full names of tickers as a list
                assignednames += namesfromtickers #add names from tickers to assignednames list
                return assignednames


            except Exception as e:
                logger.error(f"Exception in assigner(): {e}", exc_info=True)


        def get_messagecoin_recursive(message_id,d):
            """Input: message_id - ID of a message, d - temporary storage dictionary
               Implements get_messagecoin as a recursive function
               Returns:coin assigned to message or NaN"""
            #If the message has no parent, we get Exception and return messagecoin = nan
            #If we find a parent and the parent has a coin, we return messagecoin
            #If we find a parent and the parent has no coin, we call parent_coin = get_messagecoin_recursive(parent_id,teled)
            try:
                try:
                    result = d[message_id] #get message-info for current id as dict
                    messagecoin = result#["assigned_coin"]
                    if type(messagecoin) != float: #if var is not nan
                        return messagecoin #if a coin has been attributed, return
                    else:
                        message_id = result["parent_id"] #get parent id of current message
                        get_messagecoin_recursive(message_id,d) #if no coin found, look up parent message
                except (KeyError,IndexError,ValueError): #if ID not present, return NaN
                    messagecoin = np.nan
                    return messagecoin

            except Exception as e:
                logger.error(f"Exception in get_messagecoin_recursive(): {e}", exc_info=True)

        def replaceUsernameTwitter(text):
            """ Replaces "@user" with "username_placeholder" """
            text = re.sub('@[^\s]+','username_placeholder',text)
            return text

        def replaceUsernameReddit(text):
            """ Replaces "u/user" with "username_placeholder" """
            text = re.sub('u\/[^\s]+','username_placeholder',text)
            return text

        ###process flow

        toptickersallcaps = [x.upper() for x in toptickers]
        tmp = []
        for ticker in  toptickersallcaps:
            ticker = ticker.strip() #remove added whitespaces -> explanation on line 223-224
            tmp.append(ticker)
        toptickersallcaps = tmp
        assignedtickersallcaps = assigner(toptickersallcaps,text) #Get all target tickers that are in CAPS (we  assume that tickers in caps always refer to the target and not something else)
        if assignedtickersallcaps: #if allcap tickers are found, replace them with placeholders to avoid redundant counts later on
            for ticker in assignedtickersallcaps:
                text = text.replace(ticker, "allcaps_ticker_placeholder")
            assignedtickersallcaps = [" " + s + " " for s in assignedtickersallcaps]

        if type(text) == str:
            text = text.lower() #transform capital letters to lowercase letters

        """ for Twitter API & Reddit API integration

        if "Twitter" in belongs_to:
            text = replaceUsernameTwitter(text) #replace twitter @USERNAME with placeholder

        if "Reddit" in belongs_to:
            text = replaceUsernameReddit(text) #replace Reddit /U/USERNAME with placeholder
            #List of strings posted by reddit-bots
            bot_messages_list = ["i am a bot, and this action was performed automatically","^^i ^^am ^^a ^^bot","i am just a simple bot","remindmebot","crypto_bot","this summary is auto generated by a bot","lntipbot","i'm a bot"]
            if any(ext in text for ext in bot_messages_list):
                text = np.nan #if text posted by bot, remove text
        """
        parent_coin = np.nan
        pmessage = np.nan
        if "nan" not in str(parent_id): #get coin assigned to parent message if parent_id is not "nan"
            if "Telegram" in belongs_to:
                parent_coin = get_messagecoin_recursive(parent_id,teled)
                if type(parent_coin) != float:
                    pmessage = parent_coin["text"]
                    parent_coin = parent_coin["assigned_coin"]
            """
            for Twitter API & Reddit API integration
            if "Twitter" in belongs_to:
                parent_coin = get_messagecoin_recursive(parent_id,twitd)
                if type(parent_coin) != float:
                    pmessage = parent_coin["text"]
                    parent_coin = parent_coin["assigned_coin"]
            if "Reddit" in belongs_to:
                parent_coin = get_messagecoin_recursive(parent_id,redd)
                if type(parent_coin) != float:
                    pmessage = parent_coin["text"]
                    parent_coin = parent_coin["assigned_coin"]
            """
        assignednames = assigner(topnames, text) #search target names in message

        if assignednames:
        #if full names are found, replace them with placeholders to avoid redundant counts (e.g. name="solana", ticker="sol")
            for name in assignednames:
                text = text.replace(name, "target_name_placeholder")

        assignedtickers = assigner(toptickers,text) #search target tickers in message

        tmp = []
        for ticker in toptickers:
            ticker = ticker.strip() #remove added whitespaces
            tmp.append(ticker)
        #the reason for not simply removing $ and # chars in text is that tickers are only attributed if there is
        #a space before and after (" "BTC" ") to avoid FPs (line 42) - we will miss many cash/hashtags if just remove the $,# chars
        toptickercashtags = [ "$"+ s for s in tmp] #create ticker cashtags ("$BTC")
        toptickerhashtags = [ "#"+ s for s in tmp] #create ticker hashtags ("#BTC")
        toptickerctht = toptickercashtags + toptickerhashtags
        assignedctht = assigner(toptickerctht,text) #search target ticker cashtags and hashtags
        assignedtickers = assignedtickers + assignedctht + assignedtickersallcaps
        assignedtickers = [x.lower() for x in assignedtickers]


        if assignedtickers:
            assignednames = to_names(toptickers,topnames, assignedtickers, assignednames)


        if len(assignednames) == 1:
            assigned_coin = assignednames[0] #if only 1 target coin mentioned, set it as assigned coin

        if len(assignednames) > 1: #if multiple target names present in text:
            occurences = Counter(assignednames) #returns a dict with occurences of items in names list
            highest = max(occurences.values()) #get highest value in Dict
            maxoccur = [k for k, v in occurences.items() if v == highest] #Count keys that have highest value
            if len(maxoccur) == 1: #If exactly one key has the highest value, set key as assigned coin
                assigned_coin = maxoccur[0]
            else:
                if type(parent_coin) != float:
                    assigned_coin = parent_coin #set assigned coin of parent message as current assigned coin (if applicable)
                else:
                    assigned_coin = np.nan

        if len(assignednames) < 1: #if no target names present in text
            if type(parent_coin) != float:
                assigned_coin = parent_coin #set assigned coin of parent message as current assigned coin (if applicable)
            else:
                assigned_coin = np.nan

        updatelist = [msgid,timestamp,parent_id,assigned_coin,text]
        updatedict = {"msg_id":updatelist[0],"timestamp":updatelist[1],"parent_id":updatelist[2],"assigned_coin":updatelist[3],"text":updatelist[4]}

        if type(assigned_coin) == str:
            assigned_coin = assigned_coin.lower() #set assigned coin to lowercase for uniform representation

        """
        for Twitter API & Reddit API integration
        if "Twitter" in belongs_to:
            twitd[msgid] = updatedict

        if "Reddit" in belongs_to:
            redd[msgid] = updatedict
        """

        if "Telegram" in belongs_to:
            teled[msgid] = updatedict

        if "None" in str(assigned_coin):
            assigned_coin = np.nan

        return assigned_coin

    except Exception as e:
        logger.error(f"Exception in coin_to_text_assigner(): {e}", exc_info=True)
