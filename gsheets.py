from google.oauth2 import service_account
import pygsheets
import json
import env
import text_analysis
import datetime


id = env.GOOGLE_SHEET_ID

def setup():
    ###only run once to populate the authorization params
    gc = pygsheets.authorize(client_secret='gsheets-auth.json')
    return

def get_client():
    client = pygsheets.authorize()
    return client

def get_sheet():
    sheet = client.open_by_key(id)
    return sheet
    
###Twitter
def update_twitter_public_metrics(handle, twitter_public_metrics_):
    try:
        sheet_name = "Twitter Public Metrics"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = sheet.add_worksheet(sheet_name)
            current_data = []
            target_worksheet.append_table([
                "Date",
                "Handle",
                "Followers", 
                "Following", 
                "Tweet Count", 
                "Listed Count"
            ])
            target_worksheet.append_table([
                str(datetime.datetime.now()),
                handle,
                twitter_public_metrics_.get("followers_count"), 
                twitter_public_metrics_.get("following_count"), 
                twitter_public_metrics_.get("tweet_count"), 
                twitter_public_metrics_.get("listed_count")
            ])
            return {"success": True}
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
            current_data = target_worksheet.get_all_records()
            target_worksheet.append_table([
                str(datetime.datetime.now()),
                handle,
                twitter_public_metrics_.get("followers_count"), 
                twitter_public_metrics_.get("following_count"), 
                twitter_public_metrics_.get("tweet_count"), 
                twitter_public_metrics_.get("listed_count")
            ])
            return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def update_recent_tweets(handle, recent_tweets_):
    try:
        sheet_name = "Recent Tweets"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = sheet.add_worksheet(sheet_name)
            current_data = []
            target_worksheet.append_table([
                "Date",
                "CreatedAt",
                "Handle",
                "UserId",
                "Language",
                "TweetId",
                "Retweets",
                "Replies",
                "Likes",
                "Quotes",
                "Text",
                "Mentions",
                "URLs",
                "Hashtags",
                "IsRetweet"
            ])
            for r in recent_tweets_:
                target_worksheet.append_table([
                    str(datetime.datetime.now()),
                    r.get("created_at"),
                    handle,
                    r.get("author_id"), 
                    r.get("lang"),
                    r.get("id"),
                    r.get("public_metrics").get("retweet_count"),
                    r.get("public_metrics").get("reply_count"),
                    r.get("public_metrics").get("like_count"),
                    r.get("public_metrics").get("quote_count"),
                    r.get("text"),
                    ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                    ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                    ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                    "RT " == r.get("text")[:3]
                ])
            return {"success": True}
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
            current_data = target_worksheet.get_all_records()
            for r in recent_tweets_:
                current_tweet_row = [x for x in range(len(current_data)) if current_data[x].get("TweetId") == int(r.get("id"))]
                if len(current_tweet_row) > 0:
                    row_to_update = current_tweet_row[0] + 2
                    target_worksheet.update_row(row_to_update, [
                        str(datetime.datetime.now()),
                        r.get("created_at"),
                        handle,
                        r.get("author_id"), 
                        r.get("lang"),
                        r.get("id"),
                        r.get("public_metrics").get("retweet_count"),
                        r.get("public_metrics").get("reply_count"),
                        r.get("public_metrics").get("like_count"),
                        r.get("public_metrics").get("quote_count"),
                        r.get("text"),
                        ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                        ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                        ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                        "RT " == r.get("text")[:3]
                    ], 0)
                else:
                    target_worksheet.append_table([
                        str(datetime.datetime.now()),
                        r.get("created_at"),
                        handle,
                        r.get("author_id"), 
                        r.get("lang"),
                        r.get("id"),
                        r.get("public_metrics").get("retweet_count"),
                        r.get("public_metrics").get("reply_count"),
                        r.get("public_metrics").get("like_count"),
                        r.get("public_metrics").get("quote_count"),
                        r.get("text"),
                        ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                        ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                        ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                        "RT " == r.get("text")[:3]
                    ])
            return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def update_recent_mentions(handle, recent_twitter_mentions_):
    try:
        sheet_name = "Recent Mentions"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = sheet.add_worksheet(sheet_name)
            current_data = []
            target_worksheet.append_table([
                "Date",
                "CreatedAt",
                "Handle",
                "UserId",
                "Language",
                "TweetId",
                "Retweets",
                "Replies",
                "Likes",
                "Quotes",
                "Text",
                "Mentions",
                "URLs",
                "Hashtags",
                "IsRetweet"
            ])
            for r in recent_twitter_mentions_:
                target_worksheet.append_table([
                    str(datetime.datetime.now()),
                    r.get("created_at"),
                    handle,
                    r.get("author_id"), 
                    r.get("lang"),
                    r.get("id"),
                    r.get("public_metrics").get("retweet_count"),
                    r.get("public_metrics").get("reply_count"),
                    r.get("public_metrics").get("like_count"),
                    r.get("public_metrics").get("quote_count"),
                    r.get("text"),
                    ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                    ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                    ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                    "RT " == r.get("text")[:3]
                ])
            return {"success": True}
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
            current_data = target_worksheet.get_all_records()
            for r in recent_tweets_:
                current_tweet_row = [x for x in range(len(current_data)) if current_data[x].get("TweetId") == int(r.get("id"))]
                if len(current_tweet_row) > 0:
                    row_to_update = current_tweet_row[0] + 2
                    target_worksheet.update_row(row_to_update, [
                        str(datetime.datetime.now()),
                        r.get("created_at"),
                        handle,
                        r.get("author_id"), 
                        r.get("lang"),
                        r.get("id"),
                        r.get("public_metrics").get("retweet_count"),
                        r.get("public_metrics").get("reply_count"),
                        r.get("public_metrics").get("like_count"),
                        r.get("public_metrics").get("quote_count"),
                        r.get("text"),
                        ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                        ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                        ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                        "RT " == r.get("text")[:3]
                    ], 0)
                else:
                    target_worksheet.append_table([
                        str(datetime.datetime.now()),
                        r.get("created_at"),
                        handle,
                        r.get("author_id"), 
                        r.get("lang"),
                        r.get("id"),
                        r.get("public_metrics").get("retweet_count"),
                        r.get("public_metrics").get("reply_count"),
                        r.get("public_metrics").get("like_count"),
                        r.get("public_metrics").get("quote_count"),
                        r.get("text"),
                        ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                        ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                        ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                        "RT " == r.get("text")[:3]
                    ])
            return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def update_recent_mentions(t, recent_tweets_topics_):
    try:
        sheet_name = "Twitter Topic Sampling"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = sheet.add_worksheet(sheet_name)
            current_data = []
            target_worksheet.append_table([
                "Date",
                "CreatedAt",
                "Topic",
                "Handle",
                "Name",
                "UserId",
                "Language",
                "TweetId",
                "Retweets",
                "Replies",
                "Likes",
                "Quotes",
                "Text",
                "Mentions",
                "URLs",
                "Hashtags",
                "IsRetweet"
            ])
            for r in recent_tweets_topics_:
                target_worksheet.append_table([
                    str(datetime.datetime.now()),
                    r.get("created_at"),
                    t,
                    r.get("username"),
                    r.get("name"),
                    r.get("author_id"), 
                    r.get("lang"),
                    r.get("id"),
                    r.get("public_metrics").get("retweet_count"),
                    r.get("public_metrics").get("reply_count"),
                    r.get("public_metrics").get("like_count"),
                    r.get("public_metrics").get("quote_count"),
                    r.get("text"),
                    ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                    ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                    ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                    "RT " == r.get("text")[:3]
                ])
            return {"success": True}
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
            current_data = target_worksheet.get_all_records()
            for r in recent_tweets_topics_:
                current_tweet_row = [x for x in range(len(current_data)) if current_data[x].get("TweetId") == int(r.get("id"))]
                if len(current_tweet_row) > 0:
                    row_to_update = current_tweet_row[0] + 2
                    target_worksheet.update_row(row_to_update, [
                        str(datetime.datetime.now()),
                        r.get("created_at"),
                        r.get("username"),
                        r.get("name"),
                        r.get("author_id"), 
                        r.get("lang"),
                        r.get("id"),
                        r.get("public_metrics").get("retweet_count"),
                        r.get("public_metrics").get("reply_count"),
                        r.get("public_metrics").get("like_count"),
                        r.get("public_metrics").get("quote_count"),
                        r.get("text"),
                        ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                        ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                        ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                        "RT " == r.get("text")[:3]
                    ], 0)
                else:
                    target_worksheet.append_table([
                        str(datetime.datetime.now()),
                        r.get("created_at"),
                        r.get("username"),
                        r.get("name"),
                        r.get("author_id"), 
                        r.get("lang"),
                        r.get("id"),
                        r.get("public_metrics").get("retweet_count"),
                        r.get("public_metrics").get("reply_count"),
                        r.get("public_metrics").get("like_count"),
                        r.get("public_metrics").get("quote_count"),
                        r.get("text"),
                        ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "",
                        ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "",
                        ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "",
                        "RT " == r.get("text")[:3]
                    ])
            return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}
client = get_client()
