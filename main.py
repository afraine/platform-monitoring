import env
import config
import requests
import os
import json
import gsheets
import feedparser
import youtube_dl
import time
import uuid
import math
import datetime
from text_analysis import *

def update():
    """Get all updated data and metrics"""
    
    """Rate Limits:
        Google Sheets: 1 operation per second
        Twitter Search: 180 requests/15min (1800 Tweets)
        Twitter Timeline: 900 requests/15min (9000 Tweets)
    """

    success, target_twitter_handles, listening_twitter_handles, listening_twitter_topics, target_youtube_channels = gsheets.get_inputs()
    num_tweets = 0
    if success:
        time_start = time.time()
        current_recent_tweets_ = gsheets.get_current_data("Recent Tweets")
        current_recent_mentions_ = gsheets.get_current_data("Recent Mentions")
        current_twitter_topic_sampling_ = gsheets.get_current_data("Twitter Topic Sampling")
        current_youtube_videos = gsheets.get_current_data("Youtube Videos")

        #####Remove current tweets and mentions and topical tweets more than 7/30 days old
        unique_handles_recent_tweets = list(set([x.get("Handle") for x in current_recent_tweets_]))
        handles_count_recent_tweets = [{"handle": x, "count": len([y for y in current_recent_tweets_ if y.get("Handle") == x])} for x in unique_handles_recent_tweets]
        current_recent_tweets_keep = []
        for h in handles_count_recent_tweets:
            if h not in target_twitter_handles:
                if h.get("count") < 200:
                    current_recent_tweets_keep.append([x for x in current_recent_tweets_ if x.get("Handle") == h.get("handle")])
                else:
                    t = [x for x in current_recent_tweets_ if x.get("Handle") == h.get("handle")]
                    t.sort(key=lambda item:item['CreatedAt'], reverse=True)
                    current_recent_tweets_keep.append(t[:200])
            else:
                current_recent_tweets_keep.append([x for x in current_recent_tweets_ if x.get("Handle") == h.get("handle")])
        current_recent_tweets = [j for i in current_recent_tweets_keep for j in i]
        current_recent_tweets.sort(key=lambda item:item['CreatedAt'], reverse=True)
        current_recent_tweets_to_update = [x for x in current_recent_tweets if x.get("Handle") in target_twitter_handles and datetime.datetime.strptime(x.get("CreatedAt").split("T")[0], "%Y-%m-%d") >= datetime.datetime.now() - datetime.timedelta(hours=7*24)]
        try:
            updated_current_recent_tweets = tweet_lookup(current_recent_tweets_to_update) if len(current_recent_tweets_to_update) > 0 else []
            for r in updated_current_recent_tweets:
                match = [x for x in range(len(current_recent_tweets)) if current_recent_tweets[x].get("TweetId") == "id_{}".format(r.get("id"))]
                if len(match) > 0:
                    current_recent_tweets[match[0]]['Retweets'] = r.get("public_metrics").get("retweet_count")
                    current_recent_tweets[match[0]]['Replies'] = r.get("public_metrics").get("reply_count")
                    current_recent_tweets[match[0]]['Likes'] = r.get("public_metrics").get("like_count")
                    current_recent_tweets[match[0]]['Quotes'] = r.get("public_metrics").get("quote_count")
        except:
            print("there was an error updating the recent tweets")
        
        current_recent_mentions = [x for x in current_recent_mentions_ if datetime.datetime.strptime(x.get('CreatedAt').split("T")[0], "%Y-%m-%d") >= datetime.datetime.now() - datetime.timedelta(hours=30*24)]
        current_twitter_topic_sampling = [x for x in current_twitter_topic_sampling_ if datetime.datetime.strptime(x.get('CreatedAt').split("T")[0], "%Y-%m-%d") >= datetime.datetime.now() - datetime.timedelta(hours=7*24)]
        
        unique_current_recent_tweets_ids = list(set([x.get("TweetId") for x in current_recent_tweets]))
        unique_current_recent_mentions_ids = list(set([x.get("TweetId") for x in current_recent_mentions]))
        unique_current_twitter_topic_sampling_ids = list(set([x.get("TweetId") for x in current_twitter_topic_sampling]))

        current_recent_tweets = [[y for y in current_recent_tweets if x == y.get("TweetId")][0] for x in unique_current_recent_tweets_ids]
        current_recent_tweets.sort(key=lambda item:item['CreatedAt'], reverse=True)

        current_recent_mentions = [[y for y in current_recent_mentions if x == y.get("TweetId")][0] for x in unique_current_recent_mentions_ids]
        current_recent_mentions.sort(key=lambda item:item['CreatedAt'], reverse=True)

        current_twitter_topic_sampling = [[y for y in current_twitter_topic_sampling if x == y.get("TweetId")][0] for x in unique_current_twitter_topic_sampling_ids]
        current_twitter_topic_sampling.sort(key=lambda item:item['CreatedAt'], reverse=True)
        
        #####Update Twitter metrics for previously acquired tweets within the last 90 days
        # recent_tweets_to_update = [x for x in current_recent_tweets if datetime.datetime.strptime(x.get("CreatedAt").split("T")[0], "%Y-%m-%d") > datetime.datetime.now() - datetime.timedelta(days=30)]
        # recent_mentions_to_update = [x for x in current_recent_mentions if datetime.datetime.strptime(x.get("CreatedAt").split("T")[0], "%Y-%m-%d") > datetime.datetime.now() - datetime.timedelta(days=30)]
        # youtube_videos_to_update = current_youtube_videos

        # tweets_to_update = recent_tweets_to_update + recent_mentions_to_update
        # tweets_to_update.sort(key=lambda item:item['CreatedAt'], reverse=True)
        # tweets_to_update_ = tweets_to_update[:90000] #Update the most recent 90000 Tweets
        # num_batches = math.ceil(len(tweets_to_update_)/100)
        # updated_tweets = []
        # for n in range(num_batches):
        #     print("getting updates for batch {}/{}".format(n, num_batches))
        #     batch = tweets_to_update_[n*100:(n+1)*100]
        #     updated_tweets.append(tweet_lookup(batch))
        # updated_tweets_ = [j for i in updated_tweets for j in i]
        
        # current_recent_tweets_updated = [current_recent_tweets[x] for x in range(len(updated_tweets_)) if x.get("TweetId") == updated_tweets_]

    
        """tweets for input target handles"""
        new_tweets_to_add, new_mentions_to_add, new_public_metrics_to_add, new_tweets_on_topic_to_add, topic_analysis, listening_account_analysis = [], [], [], [], [], []
        for tth in target_twitter_handles[:100]:
            print("getting public twitter metrics for : {} ({} min)".format(tth, int((time.time() - time_start)/60)))
            twitter_public_metrics_ = twitter_public_metrics(tth)
            new_public_metrics_to_add.append(twitter_public_metrics_)
            print("getting recent tweets for : {} ({} min)".format(tth, int((time.time() - time_start)/60)))
            recent_tweets_, next_token = recent_tweets(tth, None, True, True, True)
            num_tweets += len(recent_tweets_)
            new_tweets_to_add.append(recent_tweets_)
            print("getting recent mentions for : {} ({} min)".format(tth, int((time.time() - time_start)/60)))
            recent_twitter_mentions_ = recent_twitter_mentions(tth, True)
            num_tweets += len(recent_twitter_mentions_)
            recent_twitter_mentions_sentiment = [sentiment(x.get("text")) for x in recent_twitter_mentions_]
            for j in range(len(recent_twitter_mentions_)):
                recent_twitter_mentions_[j]['sentiment'] = recent_twitter_mentions_sentiment[j]
            new_mentions_to_add.append(recent_twitter_mentions_)

        """tweets for target topics"""
        for t in listening_twitter_topics[:100]:
            print("getting recent tweets for topic: {} ({} min)".format(t, int((time.time() - time_start)/60)))
            recent_tweets_topics_ = recent_tweets_keyword(t, True)
            num_tweets += len(recent_tweets_topics_)
            recent_tweets_topics_sentiment = [sentiment(x.get("text")) for x in recent_tweets_topics_]
            for j in range(len(recent_tweets_topics_)):
                recent_tweets_topics_[j]['sentiment'] = recent_tweets_topics_sentiment[j]
            new_tweets_on_topic_to_add.append(recent_tweets_topics_)
            recent_tweets_topics_.sort(key=lambda item:item['created_at'])
            authors = [x.get("username") for x in recent_tweets_topics_]
            unique_authors = list(set(authors))
            author_counts = [{"count": len([x for x in authors if x == y]), "author": y} for y in unique_authors]
            author_counts.sort(key=lambda item:item['count'], reverse=True)
            keyword_counts = recent_tweet_counts(t)
            topic_analysis.append({
                "topic": t,
                "date": str(datetime.datetime.now()).split(".")[0],
                "top_accounts_by_number": [x for x in author_counts if x.get("count") > 1][:10],
                "keyword_counts_hourly": keyword_counts,
                "average_sentiment": sum(recent_tweets_topics_sentiment)/len(recent_tweets_topics_sentiment) if len(recent_tweets_topics_sentiment) > 0 else 0
            })

        """tweets for target listening accounts"""
        for t in listening_twitter_handles[:100]:
            print("getting recent tweets for account: {} ({} min)".format(t, int((time.time() - time_start)/60)))
            recent_tweets_accounts_, next_token = recent_tweets(t, None, True, True, True)
            num_tweets += len(recent_tweets_accounts_)
            new_tweets_to_add.append(recent_tweets_accounts_)
            for j in range(len(recent_tweets_accounts_)):
                if "id_{}".format(recent_tweets_accounts_[j].get("id")) not in [x.get("TweetId") for x in current_recent_tweets]:
                    listening_account_analysis.append({
                        "account": t,
                        "date": recent_tweets_accounts_[j].get("created_at"),
                        "hashtags": [],
                        "mentions": [],
                        "tweet_id": recent_tweets_accounts_[j].get("id")
                    })
                    if recent_tweets_accounts_[j].get("entities") is not None:
                        if "hashtags" in recent_tweets_accounts_[j].get("entities"):
                            listening_account_analysis[-1]['hashtags'] = [x.get("tag") for x in recent_tweets_accounts_[j].get("entities").get("hashtags")]
                        if "mentions" in recent_tweets_accounts_[j].get("entities"):
                            listening_account_analysis[-1]['mentions'] = [x.get("username") for x in recent_tweets_accounts_[j].get("entities").get("mentions")]

        # all_hashtags = [j for i in [x.get("hashtags") for x in listening_account_analysis] for j in i]
        # unique_hashtags = list(set(all_hashtags))
        # hashtag_counts = [{"count": len([x for x in all_hashtags if x == y]), "topic": y, "type": "hashtag"} for y in unique_hashtags]
        # hashtag_counts.sort(key=lambda item:item['count'], reverse=True)

        new_tweets_to_add_ = [j for i in new_tweets_to_add for j in i]
        new_mentions_to_add_ = [j for i in new_mentions_to_add for j in i]
        new_tweets_on_topic_to_add_ = [j for i in new_tweets_on_topic_to_add for j in i]

        print("adding public metrics to sheets")
        res = gsheets.add_twitter_public_metrics(new_public_metrics_to_add)
        print("adding new tweets to sheets")
        res = gsheets.add_new_tweets(current_recent_tweets + [x for x in new_tweets_to_add_ if "id_{}".format(x.get("id")) not in [y.get("TweetId") for y in current_recent_tweets]])
        print("adding new mentions to sheets")
        res = gsheets.add_new_mentions(current_recent_mentions + [x for x in new_mentions_to_add_ if "id_{}".format(x.get("id")) not in [y.get("TweetId") for y in current_recent_mentions]])
        print("adding topical tweets to sheets")  
        res = gsheets.add_new_topics(current_twitter_topic_sampling + [x for x in new_tweets_on_topic_to_add_ if "id_{}".format(x.get("id")) not in [y.get("TweetId") for y in current_twitter_topic_sampling]])
        print("adding topic analysis")
        res = gsheets.add_topic_analysis(topic_analysis)
        """videos for input youtube channel"""
        videos = [get_recent_video_links(x) for x in target_youtube_channels]
        video_links, video_titles, video_published = [], [], []
        for j in range(len(videos)):
            for k in range(len(videos[j][0])):
                if videos[j][0][k] not in [y.get("Link") for y in current_youtube_videos]:
                    video_links.append(videos[j][0][k])
                    video_titles.append(videos[j][1][k])
                    video_published.append(videos[j][2][k])
        recent_videos_ = [get_recent_videos(video_links, video_titles, video_published, x) for x in target_youtube_channels]
        recent_videos = [j for i in recent_videos_ for j in i]
        print("adding Youtube videos to sheets")
        res = gsheets.add_recent_videos(recent_videos)
        """Youtube account statistics"""
        stats = [{"channel_id": x, "stats": get_youtube_statistics(x)} for x in target_youtube_channels]
        res = gsheets.add_youtube_metrics(stats)
        res = gsheets.add_data_to_log([{"date": str(datetime.datetime.now()).split(".")[0], "num": num_tweets}])
        return True
    else:
        print("could not retrieve input params")
        return False

###Twitter###
def twitter_public_metrics(handle):
    """Return the public metrics of a twitter handle"""
    user_id = get_user_id(handle)
    if user_id is not None:
        resp = make_request("{}users/{}?user.fields={}".format(
            config.endpoints.get("twitter"),
            user_id,
            "public_metrics"
        ), config.headers.get("twitter").get("header"))
        valid_resp = validate_twitter_response(resp)
        if valid_resp:
            result = json.loads(resp.text)
            if 'data' in result:
                return_data = result.get("data").get("public_metrics")
                return_data['handle'] = handle
                return return_data
            else:
                return {"handle": handle}
        else:
            return {"handle": handle}
    else:
        return {"handle": handle}

def recent_tweets(handle, next_token_, only_new, exclude_RT, exclude_replies):
    """Return the recent tweets from a specific twitter handle"""
    user_id = get_user_id(handle)
    if user_id is not None:
        p = "created_at,context_annotations,entities,in_reply_to_user_id,lang,public_metrics,referenced_tweets,source,text,attachments&expansions=attachments.media_keys,author_id,entities.mentions.username,in_reply_to_user_id&user.fields=created_at&media.fields=preview_image_url,type,url,width,public_metrics"
        exclusions = []
        if exclude_RT:
            exclusions.append("retweets")
        if exclude_replies:
            exclusions.append("replies")
        if len(exclusions) > 0:
            p += "&exclude={}".format(",".join(exclusions))
        if only_new:
            p += "&start_time=" + datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=env.FREQUENCY), "%Y-%m-%dT%H:%M:%SZ")
        if next_token_ is not None:
            print("in here")
            resp = make_request(
                "{}users/{}/tweets?pagination_token={}&max_results=100&tweet.fields={}".format(
                    next_token_,
                    config.endpoints.get("twitter"),
                    user_id,
                    p
                ), config.headers.get("twitter").get("header")
            )
        else:
            resp = make_request(
                "{}users/{}/tweets?max_results=100&tweet.fields={}".format(
                    config.endpoints.get("twitter"),
                    user_id,
                    p
                ), config.headers.get("twitter").get("header")
            )
        valid_resp = validate_twitter_response(resp)
        if valid_resp:
            result = json.loads(resp.text)
            if 'data' in result:
                next_token = result.get("meta").get("next_token")
                return_data = result.get("data")
                for j in range(len(return_data)):
                    return_data[j]['handle'] = handle
                    return_data[j]['media'] = []
                    if "attachments" in return_data[j]:
                        attachments = return_data[j].get("attachments")
                        if attachments is not None and "media_keys" in attachments:
                            media_keys = attachments.get("media_keys")
                            if len(media_keys) > 0:
                                if "includes" in result:
                                    if result.get("includes") is not None and "media" in result.get("includes"):
                                        media = result.get("includes").get("media")
                                        matches = [[x for x in media if x.get("media_key") == y] for y in media_keys]
                                        media_matches = [j for i in matches for j in i]
                                        return_data[j]['media'] = media_matches
                return return_data, next_token
            else:
                return [], None
        else:
            return [], None
    else:
        return [], None

def recent_twitter_mentions(handle, only_new):
    """Return the recent tweets that mention a specific twitter handle"""
    user_id = get_user_id(handle)
    if user_id is not None:
        p = "created_at,context_annotations,entities,in_reply_to_user_id,lang,public_metrics,referenced_tweets,source,text,attachments&expansions=attachments.media_keys,author_id,entities.mentions.username,in_reply_to_user_id&media.fields=preview_image_url,type,url,width,public_metrics&user.fields=created_at&max_results=100"
        if only_new:
            p += "&start_time=" + datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=env.FREQUENCY), "%Y-%m-%dT%H:%M:%SZ")
        resp = make_request("{}users/{}/mentions?tweet.fields={}".format(
            config.endpoints.get("twitter"),
            user_id,
            p
        ), config.headers.get("twitter").get("header"))
        valid_resp = validate_twitter_response(resp)
        if valid_resp:
            result = json.loads(resp.text)
            if 'data' in result:
                return_data = result.get("data")
                for j in range(len(return_data)):
                    return_data[j]['handle'] = handle
                    return_data[j]['media'] = []
                    if "attachments" in return_data[j]:
                        attachments = return_data[j].get("attachments")
                        if attachments is not None and "media_keys" in attachments:
                            media_keys = attachments.get("media_keys")
                            if len(media_keys) > 0:
                                if "includes" in result:
                                    if result.get("includes") is not None and "media" in result.get("includes"):
                                        media = result.get("includes").get("media")
                                        matches = [[x for x in media if x.get("media_key") == y] for y in media_keys]
                                        media_matches = [j for i in matches for j in i]
                                        return_data[j]['media'] = media_matches
                return return_data
            else:
                return []
        else:
            return []
    else:
        return []

def recent_tweets_keyword(keyword, only_new):
    """Return a sampling of the recent tweets that mention a specific keyword or phrase"""
    p = "created_at,context_annotations,entities,in_reply_to_user_id,lang,public_metrics,referenced_tweets,source,text,attachments&expansions=attachments.media_keys,author_id,entities.mentions.username,in_reply_to_user_id&media.fields=preview_image_url,type,url,width,public_metrics&user.fields=username,name,created_at&max_results=20"
    if only_new:
        p += "&start_time=" + datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=env.FREQUENCY), "%Y-%m-%dT%H:%M:%SZ")
    resp = make_request(
        "{}tweets/search/recent?query={}%20-is:retweet&tweet.fields={}".format(
            config.endpoints.get("twitter"),
            keyword,
            p
        ), config.headers.get("twitter").get("header")
    )
    valid_resp = validate_twitter_response(resp)
    if valid_resp:
        result = json.loads(resp.text)
        if 'data' in result:
            return_data = result.get("data")
            for j in range(len(return_data)):
                match = [x for x in result.get("includes").get("users") if x.get("id") == return_data[j].get("author_id")]
                if len(match) > 0:
                    match_ = match[0]
                    return_data[j]['username'] = match_.get("username")
                    return_data[j]['name'] = match_.get("name")
                return_data[j]['keyword'] = keyword
            return return_data
        else:
            return []
    else:
        return []

def recent_tweet_counts(keyword):
    """Return a count of the recent tweets that mention a specific keyword or phrase"""
    print("getting tweet count for: {}".format(keyword))
    print("request: {}".format("{}tweets/counts/recent?query={}%20-is:retweet&granularity=hour&start_time={}&end_time={}".format(
            config.endpoints.get("twitter"),
            keyword,
            datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=env.FREQUENCY), "%Y-%m-%dT%H:%M:%SZ"),
            datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=1), "%Y-%m-%dT%H:%M:%SZ")
        )
    ))
    resp = make_request(
        "{}tweets/counts/recent?query={}%20-is:retweet&granularity=hour&start_time={}&end_time={}".format(
            config.endpoints.get("twitter"),
            keyword,
            datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=env.FREQUENCY+1), "%Y-%m-%dT%H:%M:%SZ"),
            datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=1), "%Y-%m-%dT%H:%M:%SZ")
        ), config.headers.get("twitter").get("header")
    )
    valid_resp = validate_twitter_response(resp)
    if valid_resp:
        result = json.loads(resp.text)
        if 'data' in result:
            if len(result.get("data")) > 0:
                time_diff = datetime.datetime.strptime(result.get("data")[-1].get("end"), "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.datetime.strptime(result.get("data")[0].get("start"), "%Y-%m-%dT%H:%M:%S.%fZ")
                if time_diff.seconds > 0:
                    counts_per_hour = 60*60*sum([x.get("tweet_count") for x in result.get("data")])/(time_diff.seconds)
                else:
                    counts_per_hour = 0
            else:
                counts_per_hour = 0
            return counts_per_hour
        else:
            return 0
    else:
        return 0

def get_user_id(handle):
    """Convert a twitter handle to a twitter id"""
    resp = make_request(
        "{}users/by/username/{}".format(
            config.endpoints.get("twitter"),
            handle.replace("@", "")
        ), config.headers.get("twitter").get("header")
    )
    valid_resp = validate_twitter_response(resp)
    if valid_resp:
        result = json.loads(resp.text)
        if 'data' in result:
            if 'id' in result.get("data"):
                return result.get("data").get("id")
    return

def get_user_handle(id):
    """Convert a twitter id to a twitter handle"""
    resp = make_request(
        "{}users/{}".format(
            config.endpoints.get("twitter"),
            id
        ), config.headers.get("twitter").get("header")
    )
    valid_resp = validate_twitter_response(resp)
    if valid_resp:
        result = json.loads(resp.text)
        if 'data' in result:
            if 'id' in result.get("data"):
                return result.get("data").get("id")
    return

def validate_twitter_response(resp):
    """validate a twitter response and print out the error message if applicable"""
    if resp is None:
        return False
    else:
        if resp.status_code == 200:
            return True
        elif resp.status_code == 401:
            print(resp.status_code)
            print(resp.reason)
            print(resp.text)
            return False
        elif resp.status_code == 400:
            print(resp.status_code)
            print(resp.reason)
            print(resp.text)
            return False
        else:
            print(resp.status_code)
            return False

def get_twitter_followers(handle):
    keep_going = True
    next_cursor = None
    page_num = 1
    followers_ = []
    while keep_going:
        if next_cursor is None:
            twitter_url = "https://api.twitter.com/1.1/followers/list.json?cursor=-1&screen_name={}&count=200".format(handle)
        else:
            twitter_url = "https://api.twitter.com/1.1/followers/list.json?screen_name={}&cursor={}&count=200".format(handle, next_cursor)
        a = requests.get(twitter_url, headers=config.headers.get("twitter").get("header"))
        if a.status_code == 200:
            text = json.loads(a.text)
            page_num += 1
            followers = text.get("users")
            followers_.append(followers)
            if len(followers) < 200 or page_num == 60:
                keep_going = False
            next_cursor = text.get("next_cursor")
        elif a.status_code == 429:
            print("rate limited...sleeping for 3 minutes")
        time.sleep(3)
    return [j for i in followers_ for j in i]

def tweet_lookup(tweets):
    """Return the tweets from a specific TweetId"""
    batch_size = 100
    num_batches = math.ceil(len(tweets)/batch_size)
    updated = []
    for j in range(num_batches):
        batch = tweets[(j*batch_size):((j+1)*batch_size)]
        resp = make_request(
            "{}tweets?ids={}&tweet.fields={}".format(
                config.endpoints.get("twitter"),
                ",".join([x.get("TweetId").replace("id_","") for x in batch]),
                "created_at,context_annotations,entities,in_reply_to_user_id,lang,public_metrics,referenced_tweets,source,attachments&expansions=attachments.media_keys,author_id,entities.mentions.username,in_reply_to_user_id&user.fields=created_at"
            ), config.headers.get("twitter").get("header")
        )
        valid_resp = validate_twitter_response(resp)
        if valid_resp:
            result = json.loads(resp.text)
            if 'data' in result:
                return_data = result.get("data")
                for j in range(len(return_data)):
                    match = [x for x in tweets if x.get("TweetId").replace("id_","") == return_data[j].get("id")]
                    if len(match) > 0:
                        match_ = match[0]
                        return_data[j]['handle'] = match_.get("Handle")
                updated.append(return_data)
            else:
                updated.append([])
        else:
            updated.append([])
    return [j for i in updated for j in i]


def make_request(url, headers):
    try:
        resp = requests.get(url, headers=headers, timeout = 10)
    except:
        try:
            time.sleep(10)
            resp = requests.get(url, headers=headers, timeout = 10)
        except:
            resp = None
    return resp
###Youtube###
def get_recent_videos(video_links, video_titles, video_published, channel_id):
    num_videos = len(video_links)
    all_video_data = []
    for j in range(num_videos):
        try:
            video_data = extract_youtube_data(video_links[j])
            all_video_data.append({
                "channel_id": channel_id,
                "link": video_links[j],
                "title": video_titles[j],
                "publish_date": video_published[j],
                "meta": video_data
            })
        except Exception as e:
            print(e)
    return all_video_data

def get_recent_video_links(channel_id):
    video_links, video_titles, video_published = get_youtube_channel_video_links(channel_id)
    return video_links, video_titles, video_published

def extract_youtube_data(youtube_link):
    filename = str(uuid.uuid1())
    ydl_opts = {
        'format': 'mp4',
        'outtmpl': '/tmp/{}.mp4'.format(filename),        
        'noplaylist' : True
    }
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        meta = ydl.extract_info(youtube_link, download=False)
    return {
        "like_count": meta.get("like_count"),
        "view_count": meta.get("view_count"),
        "duration": meta.get("duration"),
        "description": meta.get("description"),
        "categories": meta.get("categories"),
        "tags": meta.get("tags")
    }

def get_youtube_statistics(channel_id):
    res_ = requests.get("https://www.googleapis.com/youtube/v3/channels?part=statistics&id={}&key={}".format(channel_id, env.YOUTUBE_API_KEY))
    if res_.status_code == 200:
        statistics = json.loads(res_.text)
        if "items" in statistics:
            items = [x for x in statistics.get("items") if x.get("kind") == "youtube#channel"]
            if len(items) > 0:
                stats = items[0].get("statistics")
                return stats
    return

def get_youtube_channel_video_links(channel_id):
    feed_link = "https://www.youtube.com/feeds/videos.xml?channel_id={}".format(channel_id)
    entries = feedparser.parse(feed_link)
    if "entries" in entries:
        links = [x.get("link") for x in entries.get("entries")]
        titles = [x.get("title") for x in entries.get("entries")]
        published = [x.get("published").split("T")[0] for x in entries.get("entries")]
    else:
        links = []
        titles = []
        published = []
    return links, titles, published