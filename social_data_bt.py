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

TWITTER_HANDLES_PATH = env.TWITTER_HANDLES_PATH
TWITTER_TOPICS_PATH = env.TWITTER_TOPICS_PATH
TWITTER_TARGET_ACCOUNTS_PATH = env.TWITTER_TARGET_ACCOUNTS_PATH

def update(handle, channel):
    """Get all updated data and metrics"""

    """tweets for input handle"""
    twitter_public_metrics_ = twitter_public_metrics(handle)
    res = update_twitter_public_metrics(handle, twitter_public_metrics_)
    recent_tweets_ = recent_tweets(handle)
    res = update_recent_tweets(handle, recent_tweets_)
    recent_twitter_mentions_ = recent_twitter_mentions(handle)
    res = update_recent_mentions(handle, recent_twitter_mentions_)

    """tweets for target topics"""
    target_topics = get_target_topics()
    for t in target_topics:
        recent_tweets_topics_ = recent_tweets_keyword(t)
        res = update_recent_mentions(t, recent_tweets_topics_)

    """tweets for target accounts"""
    target_accounts = get_target_accounts()
    for t in target_accounts:
        recent_tweets_accounts_ = recent_tweets(t)
        res = update_recent_tweets(t, recent_tweets_accounts_)

    """videos for input youtube channel"""
    recent_videos = get_recent_videos(channel)
    for v in recent_videos:
        res = update_recent_videos(channel, v)

    """followers for target accounts"""
    followers = []
    for t in target_accounts:
        followers.append(get_twitter_followers(t))
    
   

    

    

###Twitter###
def twitter_public_metrics(handle):
    """Return the public metrics of a twitter handle"""
    user_id = get_user_id(handle)
    if user_id is not None:
        resp = requests.get("{}users/{}?user.fields={}".format(
            config.endpoints.get("twitter"),
            user_id,
            "public_metrics"
        ), headers=config.headers.get("twitter").get("header"))
        valid_resp = validate_twitter_response(resp)
        if valid_resp:
            result = json.loads(resp.text)
            if 'data' in result:
                return result.get("data").get("public_metrics")
            else:
                return {}
        else:
            return {}
    else:
        return {}

def recent_tweets(handle):
    """Return the recent tweets from a specific twitter handle"""
    user_id = get_user_id(handle)
    if user_id is not None:
        resp = requests.get("{}users/{}/tweets?tweet.fields={}".format(
            config.endpoints.get("twitter"),
            user_id,
            "created_at,context_annotations,entities,in_reply_to_user_id,lang,public_metrics,referenced_tweets,source,text&expansions=author_id,entities.mentions.username,in_reply_to_user_id&user.fields=created_at&max_results=20"
        ), headers=config.headers.get("twitter").get("header"))
        valid_resp = validate_twitter_response(resp)
        if valid_resp:
            result = json.loads(resp.text)
            if 'data' in result:
                return result.get("data")
        else:
            return {}
    else:
        return {}

def recent_twitter_mentions(handle):
    """Return the recent tweets that mention a specific twitter handle"""
    user_id = get_user_id(handle)
    if user_id is not None:
        resp = requests.get("{}tweets/search/recent?query={}&tweet.fields={}".format(
            config.endpoints.get("twitter"),
            handle,
            "created_at,context_annotations,entities,in_reply_to_user_id,lang,public_metrics,referenced_tweets,source,text&expansions=author_id,entities.mentions.username,in_reply_to_user_id&user.fields=created_at&max_results=20"
        ), headers=config.headers.get("twitter").get("header"))
        valid_resp = validate_twitter_response(resp)
        if valid_resp:
            result = json.loads(resp.text)
            if 'data' in result:
                return result.get("data")
        else:
            return {}
    else:
        return {}

def recent_tweets_keyword(keyword):
    """Return a sampling of the recent tweets that mention a specific keyword or phrase"""
    resp = requests.get("{}tweets/search/recent?query={}&tweet.fields={}".format(
        config.endpoints.get("twitter"),
        keyword,
        "created_at,context_annotations,entities,in_reply_to_user_id,lang,public_metrics,referenced_tweets,source,text&expansions=author_id,entities.mentions.username,in_reply_to_user_id&user.fields=username,name,created_at&max_results=20"
    ), headers=config.headers.get("twitter").get("header"))
    valid_resp = validate_twitter_response(resp)
    if valid_resp:
        result = json.loads(resp.text)
        if 'data' in result:
            for j in range(len(result.get("data"))):
                result.get("data")[j]['username'] = result.get("includes").get("users")[j].get("username")
                result.get("data")[j]['name'] = result.get("includes").get("users")[j].get("name")
            return result.get("data")
        else:
            return []
    else:
        return []

def update_tweets_from_targets():
    target_handles = get_target_accounts()
    for t in target_handles[3:]:
        print("adding recent tweets by {}".format(t))
        recents = recent_tweets(t)
        res = add_recent_tweets_to_gs(t, recents)

def get_target_accounts():
    fs = open(TWITTER_TARGET_ACCOUNTS_PATH)
    data = fs.read()
    handles = data.split("\n")
    handles_ = [x.split("/")[-1] for x in handles]
    return handles_

def get_target_topics():
    fs = open(TWITTER_TOPICS_PATH)
    data = fs.read()
    topics = data.split("\n")
    topics_ = [x.split("/")[-1] for x in topics]
    return topics_

def get_user_id(handle):
    """Convert a twitter handle to a twitter id"""
    resp = requests.get("{}users/by/username/{}".format(
        config.endpoints.get("twitter"),
        handle.replace("@", "")
    ), headers=config.headers.get("twitter").get("header"))
    valid_resp = validate_twitter_response(resp)
    if valid_resp:
        result = json.loads(resp.text)
        if 'data' in result:
            if 'id' in result.get("data"):
                return result.get("data").get("id")
    return

def get_user_handle(id):
    """Convert a twitter id to a twitter handle"""
    resp = requests.get("{}users/{}".format(
        config.endpoints.get("twitter"),
        id
    ), headers=config.headers.get("twitter").get("header"))
    valid_resp = validate_twitter_response(resp)
    if valid_resp:
        result = json.loads(resp.text)
        if 'data' in result:
            if 'id' in result.get("data"):
                return result.get("data").get("id")
    return

def validate_twitter_response(resp):
    """validate a twitter response and print out the error message if applicable"""
    if resp.status_code == 200:
        return True
    elif resp.status_code == 401:
        print(resp.reason)
        return False
    elif resp.status_code == 400:
        print(resp.reason)
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
###Youtube###
def get_recent_videos(channel):
    video_links, video_titles, video_published = get_youtube_channel_video_links(channel_id)
    num_videos = len(video_links)
    all_video_data = []
    for j in range(num_videos):
        try:
            video_data = extract_youtube_data(video_links[j])
            all_video_data.append({
                "link": video_links[j],
                "title": video_titles[j],
                "publish_date": video_published[j],
                "meta": video_data
            })
        except Exception as e:
            print(e)
    return all_video_data

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