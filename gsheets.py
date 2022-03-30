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
def add_twitter_public_metrics(twitter_public_metrics_):
    try:
        sheet_name = "Twitter Public Metrics"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
            target_worksheet.append_table([
                "Date",
                "Handle",
                "Followers", 
                "Following", 
                "Tweet Count", 
                "Listed Count"
            ])
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        target_worksheet.append_table([
            [
                str(datetime.datetime.now()),
                r.get("handle"),
                r.get("followers_count"), 
                r.get("following_count"), 
                r.get("tweet_count"), 
                r.get("listed_count")
            ] for r in twitter_public_metrics_
        ])
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def add_topic_analysis(topic_analysis):
    try:
        sheet_name = "Twitter Topic Analysis"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
            target_worksheet.append_table([
                "Date",
                "Topic",
                "Top Accounts",
                "Average Sentiment",
                "Keyword Count Hourly"
            ])
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        target_worksheet.append_table([
            [
                r.get("date"),
                r.get("topic"),
                ",".join([x.get("author") for x in r.get("top_accounts_by_number")]),
                r.get("average_sentiment"),
                r.get("keyword_counts_hourly")
            ] for r in topic_analysis
        ])
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def add_youtube_metrics(stats):
    try:
        sheet_name = "Youtube Metrics"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
            target_worksheet.append_table([
                "Date",
                "ChannelId",
                "Views", 
                "Subscribers", 
                "Videos"
            ])
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        recs = [
            [
                str(datetime.datetime.now()),
                r.get("channel_id"),
                r.get("stats").get("viewCount"),
                r.get("stats").get("subscriberCount"),
                r.get("stats").get("videoCount")
            ] for r in stats if r is not None
        ]
        if len(recs) > 0:
            res = target_worksheet.append_table(recs, overwrite = True)
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def add_listening_account_hashtag_counts(hashtag_counts, num):
    try:
        sheet_name = "Influencer Topic Analysis"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
            target_worksheet.append_table([
                "Date",
                "Topic",
                "Average Intertweet Time (s)", 
                "Tweet Velocity (/min)", 
                "Number of Tweets Sampled", 
                "Top Accounts"
            ])
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        target_worksheet.append_table([
            [
                str(datetime.datetime.now()).split(".")[0],
                r.get("topic"),
                r.get("avg_intertweet_time"), 
                r.get("tweet_velocity_per_minute"), 
                num, 
                ",".join([x.get("author") for x in r.get("top_accounts_by_number")])
            ] for r in hashtag_counts
        ])
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def add_new_tweets(recent_tweets_):
    try:
        sheet_name = "Recent Tweets"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        recs = [
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
            "Tweet",
            "Mentions",
            "URLs",
            "Hashtags",
            "IsRetweet"
        ]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        model_cell = pygsheets.Cell("A1")
        model_cell.set_number_format(
            format_type = pygsheets.FormatType.NUMBER,
            pattern = "0"
        )
        dr = pygsheets.datarange.DataRange(start="D2", end="D1000000", worksheet=target_worksheet)
        dr.apply_format(cell=model_cell)
        dr = pygsheets.datarange.DataRange(start="F2", end="F1000000", worksheet=target_worksheet)
        dr.apply_format(cell=model_cell)
        recs_ = [
            [
                str(datetime.datetime.now()),
                r.get("created_at") if r.get("created_at") is not None else r.get("CreatedAt"),
                r.get("handle") if r.get("handle") is not None else r.get("Handle"),
                r.get("author_id") if r.get("author_id") is not None else r.get("UserId"), 
                r.get("lang") if r.get("lang") is not None else r.get("Language"),
                r.get("id") if r.get("id") is not None else r.get("TweetId"),
                r.get("public_metrics").get("retweet_count") if r.get("public_metrics") is not None else r.get("Retweets"),
                r.get("public_metrics").get("reply_count") if r.get("public_metrics") is not None else r.get("Replies"),
                r.get("public_metrics").get("like_count") if r.get("public_metrics") is not None else r.get("Likes"),
                r.get("public_metrics").get("quote_count") if r.get("public_metrics") is not None else r.get("Quotes"),
                r.get("text") if r.get("text") is not None else r.get("Tweet"),
                ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "" if r.get("entities") is not None else r.get("Mentions"),
                ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "" if r.get("entities") is not None else r.get("URLs"),
                ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "" if r.get("entities") is not None else r.get("Hashtags"),
                "RT " == str(r.get("text"))[:3] if r.get("text") is not None else "RT " == str(r.get("Tweet"))[:3]
            ] for r in recent_tweets_
        ]
        recs_.insert(0, recs)
        if len(recs_) > 0:
            _ = target_worksheet.clear("A1")
            res = target_worksheet.append_table(recs_, overwrite = True, start="A1")
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}
        
def add_new_mentions(recent_twitter_mentions_):
    try:
        sheet_name = "Recent Mentions"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        model_cell = pygsheets.Cell("A1")
        model_cell.set_number_format(
            format_type = pygsheets.FormatType.NUMBER,
            pattern = "0"
        )
        dr = pygsheets.datarange.DataRange(start="D2", end="D1000000", worksheet=target_worksheet)
        dr.apply_format(cell=model_cell)
        dr = pygsheets.datarange.DataRange(start="F2", end="F1000000", worksheet=target_worksheet)
        dr.apply_format(cell=model_cell)
        recs = [
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
            "Tweet",
            "Mentions",
            "URLs",
            "Hashtags",
            "IsRetweet",
            "Sentiment"
        ]
        recs_ = [
            [
                str(datetime.datetime.now()),
                r.get("created_at") if r.get("created_at") is not None else r.get("CreatedAt"),
                r.get("handle") if r.get("handle") is not None else r.get("Handle"),
                r.get("author_id") if r.get("author_id") is not None else r.get("UserId"), 
                r.get("lang") if r.get("lang") is not None else r.get("Language"),
                r.get("id") if r.get("id") is not None else r.get("TweetId"),
                r.get("public_metrics").get("retweet_count") if r.get("public_metrics") is not None else r.get("Retweets"),
                r.get("public_metrics").get("reply_count") if r.get("public_metrics") is not None else r.get("Replies"),
                r.get("public_metrics").get("like_count") if r.get("public_metrics") is not None else r.get("Likes"),
                r.get("public_metrics").get("quote_count") if r.get("public_metrics") is not None else r.get("Quotes"),
                r.get("text") if r.get("text") is not None else r.get("Tweet"),
                ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "" if r.get("entities") is not None else r.get("Mentions"),
                ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "" if r.get("entities") is not None else r.get("URLs"),
                ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "" if r.get("entities") is not None else r.get("Hashtags"),
                "RT " == str(r.get("text"))[:3] if r.get("text") is not None else "RT " == str(r.get("Tweet"))[:3],
                r.get("sentiment") if r.get("sentiment") is not None else (r.get("Sentiment") if r.get("Sentiment") is not None else 0)
            ] for r in recent_twitter_mentions_
        ]
        recs_.insert(0, recs)
        if len(recs_) > 0:
            target_worksheet.clear("A1")
            target_worksheet.append_table(recs_, overwrite=True, start="A1")
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def add_new_topics(recent_tweets_topics_):
    try:
        sheet_name = "Twitter Topic Sampling"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        model_cell = pygsheets.Cell("A1")
        model_cell.set_number_format(
            format_type = pygsheets.FormatType.NUMBER,
            pattern = "0"
        )
        dr = pygsheets.datarange.DataRange(start="H2", end="H1000000", worksheet=target_worksheet)
        dr.apply_format(cell=model_cell)
        dr = pygsheets.datarange.DataRange(start="F2", end="F1000000", worksheet=target_worksheet)
        dr.apply_format(cell=model_cell)
        recs = [
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
            "Tweet",
            "Mentions",
            "URLs",
            "Hashtags",
            "IsRetweet",
            "Sentiment"
        ]
        recs_ = [
            [
                str(datetime.datetime.now()),
                r.get("created_at") if r.get("created_at") is not None else r.get("CreatedAt"),
                r.get("keyword") if r.get("keyword") is not None else r.get("Topic"),
                r.get("username") if r.get("handle") is not None else r.get("Handle"),
                r.get("name") if r.get("handle") is not None else r.get("Name"),
                r.get("author_id") if r.get("author_id") is not None else r.get("UserId"), 
                r.get("lang") if r.get("lang") is not None else r.get("Language"),
                r.get("id") if r.get("id") is not None else r.get("TweetId"),
                r.get("public_metrics").get("retweet_count") if r.get("public_metrics") is not None else r.get("Retweets"),
                r.get("public_metrics").get("reply_count") if r.get("public_metrics") is not None else r.get("Replies"),
                r.get("public_metrics").get("like_count") if r.get("public_metrics") is not None else r.get("Likes"),
                r.get("public_metrics").get("quote_count") if r.get("public_metrics") is not None else r.get("Quotes"),
                r.get("text") if r.get("text") is not None else r.get("Tweet"),
                ",".join([x.get("username") for x in r.get("entities").get("mentions")]) if (r.get("entities") is not None and "mentions" in r.get("entities")) else "" if r.get("entities") is not None else r.get("Mentions"),
                ",".join([x.get("expanded_url") for x in r.get("entities").get("urls")]) if (r.get("entities") is not None and "urls" in r.get("entities")) else "" if r.get("entities") is not None else r.get("URLs"),
                ",".join([x.get("tag") for x in r.get("entities").get("hashtags")]) if (r.get("entities") is not None and "hashtags" in r.get("entities")) else "" if r.get("entities") is not None else r.get("Hashtags"),
                "RT " == str(r.get("text"))[:3] if r.get("text") is not None else "RT " == str(r.get("Tweet"))[:3],
                r.get("sentiment") if r.get("sentiment") is not None else (r.get("Sentiment") if r.get("Sentiment") is not None else 0)
            ] for r in recent_tweets_topics_
        ]
        recs_.insert(0, recs)
        if len(recs_) > 0:
            target_worksheet.clear("A1")
            target_worksheet.append_table(recs_, overwrite=True, start="A1")
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def add_recent_videos(recent_videos):
    try:
        sheet_name = "Youtube Videos"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
            target_worksheet.append_table([
                "Date",
                "CreatedAt",
                "ChannelId",
                "Title",
                "Link",
                "Likes",
                "Views",
                "Duration",
                "Description",
                "Categories",
                "Tags"
            ])
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        recs = [
            [
                str(datetime.datetime.now()),
                r.get("publish_date"),
                r.get("channel_id"),
                r.get("title"),
                r.get("link"),
                r.get("meta").get("like_count"), 
                r.get("meta").get("view_count"),
                r.get("meta").get("duration"),
                r.get("meta").get("description"),
                ", ".join(r.get("meta").get("categories")),
                ", ".join(r.get("meta").get("tags"))
            ] for r in recent_videos
        ]
        if len(recs) > 0:
            target_worksheet.append_table(recs)
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def get_current_data(sheet_name):
    try:
        target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        current_data = target_worksheet.get_all_records()
        return current_data
    except Exception as e:
        print(e)
        return []

def add_data_to_log(data_log):
    try:
        sheet_name = "Data Acquisition Log"
        worksheets = get_sheet().worksheets()
        sheet_names = [x.title for x in worksheets]
        target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
        if len(target_worksheet_) == 0:
            ###create new worksheet
            target_worksheet = get_sheet().add_worksheet(sheet_name)
            target_worksheet.append_table([
                "Date",
                "Number of Tweets"
            ])
        else:
            target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        recs = [
            [
                r.get("date"),
                r.get("num")
            ] for r in data_log
        ]
        if len(recs) > 0:
            target_worksheet.append_table(recs)
        return {"success": True}
    except Exception as e:
        print(e)
        return {"success": False, "error": e.args[0]}

def get_inputs():
    sheet_name = "Inputs"
    worksheets = get_sheet().worksheets()
    sheet_names = [x.title for x in worksheets]
    target_worksheet_ = [x for x in worksheets if x.title == sheet_name]
    if len(target_worksheet_) == 0:
        return False, [], [], [], []
    else:
        target_worksheet = get_sheet().worksheet_by_title(sheet_name)
        current_data = target_worksheet.get_all_values()
        target_handles = [x[0] for x in current_data[1:] if len(x[0]) > 0]
        listening_handles = [x[1] for x in current_data[1:] if len(x[1]) > 0]
        listening_topics = [x[2] for x in current_data[1:] if len(x[2]) > 0]
        target_youtube_channels = [x[3] for x in current_data[1:] if len(x[3]) > 0]
        return True, target_handles, listening_handles, listening_topics, target_youtube_channels
client = get_client()
