import redis
from db_query import *
import time
from youtube2srt import audio2text, SubtitleDownloader
from summarizer import SrtSummarizer
import asyncio
import json
import telegra_ph
import utils

__DEV__ = False
__DEV_COUNT__ = 0


async def video_pool_update_task(t, config, redis_client, video_pool_name):
    print("Running video_pool_update_task")
    conn: MySQLClientConnection = MySQLClientConnection(config['mysql_info'])

    while True:
        t0 = time.time()
        print("start video_pool_update_task")
        await update_video_pool(conn, redis_client, video_pool_name)
        if time.time() - t0 > t:
            print(
                f"Warning: video pool task takes too long time, longer than timer interval {t} seconds")
            print(
                "This should never happen, because summerizer always works slower than the video pool update task!")
        await asyncio.sleep(t)


async def video_summerizer_task(config, redis_client, video_pool_name):
    print("Running summerizer task")
    conn: MySQLClientConnection = MySQLClientConnection(config['mysql_info'])

    while True:
        video_pool = redis_client.hgetall(video_pool_name)
        if not video_pool:
            await asyncio.sleep(20)  # sleep 20 seconds if video pool is empty
        await video_summerizer(conn, config, redis_client, video_pool_name)


def send_telegram_message(token, chat_id, message):
    """
    给 Telegram 用户发送消息。

    :param token: Telegram Bot 的访问 Token。
    :param chat_id: 接收消息的用户 ID。
    :param message: 要发送的消息内容。
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": chat_id, "text": message, 'parse_mode': 'HTML'}

    response = utils.get_http_responce(url, 'POST', params)

    if response.status != 200:
        print(
            f"Error: telegram message sent failed! status_code={response.status}, text={response.text}")
        print("message:\n", message)
    return response


async def update_video_pool(conn, redis_client: redis.Redis, video_pool_name: str):
    print("Running update_video_pool")
    all_channels = conn.select_data_from_database("user_channel")
    print("all_channels: ")
    for channel in all_channels:
        print("processing channel: ", channel["channel_name"])
        videos = await get_video_list(conn, channel)
        if videos:
            if redis_client.exists(channel['channel_url']):
                old_videos = json.loads(redis_client.hget(
                    video_pool_name, channel['channel_url']))
                old_videos.extend(videos)
                redis_client.hset(
                    video_pool_name, channel['channel_url'], json.dumps(old_videos))
            else:
                redis_client.hset(
                    video_pool_name, channel['channel_url'], json.dumps(videos))

            # async with lock:
            #     if channel['channel_url'] in video_pool:
            #         video_pool[channel['channel_url']].extend(videos)
            #     else:
            #         video_pool[channel['channel_url']] = videos

            # print(json.loads(redis_client.hget(video_pool_name, channel['channel_url'])))
    return


async def get_video_list(conn, channel):
    # user_channel=await select_data_from_database(conn, "user_channel", tg_user_id=channel["tg_user_id"], channel_url=channel["channel_url"])
    # user_channel=user_channel[0]

    old_video_time = channel["newest_video_time"]
    now = time.time() + time.altzone

    res = utils.get_http_responce(
        "https://rsshub.app/" + channel["channel_url"] + "?format=json", 'GET', None)

    data = json.loads(res.data)

    result = []
    for item in data["items"]:

        pubDate = item["date_published"]
        time_tuple = time.strptime(pubDate, "%Y-%m-%dT%H:%M:%S.%fZ")
        t1 = time.mktime(time_tuple)

        if not __DEV__:
            if t1 <= old_video_time:  # only process video published after last processed video
                break
            if now - t1 > 3600 * 24:  # only process video published in 1 day
                break

        print(f"channel {channel['channel_name']}: ",
              f'New video {item["title"]} found, adding to video pool!')
        result.append({"title": item["title"], "link": item["url"], "pubDate": t1, "tg_user_id": channel["tg_user_id"],
                       "channel_name": channel["channel_name"]})  # use timestamp as pubDate

        if __DEV__:
            global __DEV_COUNT__
            if __DEV_COUNT__ == 1:
                break
            else:
                __DEV_COUNT__ += 1

    return result


def video_pool_is_empty(video_pool):
    x = False
    for channel in video_pool:
        x = x or json.loads(video_pool[channel])
    return not x


async def video_summerizer(conn, config, redis_client: redis.Redis, video_pool_name: str):
    video_pool = redis_client.hgetall(video_pool_name)

    model = config['faster_whisper']['model']  # default large-v2
    gpu = config['faster_whisper']['gpu_index']
    audio2text_tool = audio2text(model, gpu)  # support gpu only, cpu too slow

    # 创建字幕下载器实例
    downloader = SubtitleDownloader(config['youtube_dl'], audio2text_tool)
    srt_summarize = SrtSummarizer(config["openai"])

    if not video_pool:
        await asyncio.sleep(20)  # sleep 20 seconds if video pool is empty
    if video_pool_is_empty(video_pool):
        print("video pool is empty, sleeping for 3 minutes !")
        await asyncio.sleep(60 * 3)  # sleep 3 minutes if video pool is empty

    for channel in video_pool:
        videos = json.loads(video_pool[channel])

        print(f"start summerize channel {channel}")
        if not videos:
            print(f"video pool for channel {channel} is empty!")
            continue

        user_channel = conn.select_data_from_database("user_channel", channel_url=channel,
                                                      tg_user_id=videos[0]["tg_user_id"])
        if user_channel:
            old_video_time = user_channel[0]["newest_video_time"]

        new_video_time = -1

        while videos:

            video = videos.pop(0)
            redis_client.hset(video_pool_name, channel, json.dumps(videos))

            video_from_database = conn.select_data_from_database(
                "user_video", video_url=video['link'])
            # 如果视频库中存在该视频，则直接输出相关信息，并跳过本次循环
            if video_from_database:

                srt_url = video_from_database[0]["srt_url"]
                edit_url = video_from_database[0]["edit_url"]
                result = video_from_database[0]['result']

                tg_message = f'<b>{video["channel_name"]}\n</b>' \
                    + f'<u>{video["title"]}\n</u>' \
                    + f'👉<a href="{srt_url}" >字幕(subtitle)</a>' \
                    + f'👉<a href="{edit_url}">全文(fulltext)</a>\n' \
                    + result

                res = send_telegram_message(
                    config["telegram_bot"]["token"], video["tg_user_id"], tg_message)
                if res.status != 200:
                    print(
                        f"Error: telegram message sent failed! status_code={res.status}, text={res.text}")
                    continue
                else:
                    print(
                        f"video {video['title']} summerized and sent to user {video['tg_user_id']}!")

                if video_from_database[0]['tg_user_id'] != video["tg_user_id"]:
                    myself_data = conn.select_data_from_database(
                        "user_video", video_url=video['link'], tg_user_id=video["tg_user_id"])
                    if not myself_data:
                        conn.insert_data_to_database("user_video", tg_user_id=video['tg_user_id'], video_url=video['link'],
                                                     channel_name=video['channel_name'], title=video['title'], srt_url=srt_url, edit_url=edit_url, result=result)

                continue

            if user_channel:
                if video["pubDate"] <= old_video_time:
                    print(
                        'Error: video pubDate is older than newest_time, skip this channel. This is not supposed to happen, please check the code!')
                    break

                if video["pubDate"] > new_video_time:
                    new_video_time = video["pubDate"]

            srt = downloader.get_subtitles(video["link"])
            if srt is None:
                print(
                    f"Error: no subtitle found for video {video['title']}, skip this video!")
                print("This may because it is a live video !")
                continue

            paragraphs = srt_summarize.edit(srt)
            result = srt_summarize.summarize(paragraphs)

            # if result is not None:  ##大部分时间都不应该出现空白的问题
            srt_url = telegra_ph.publish_srt_to_telegraph(
                config["telegra.ph"]["access_token"], video["title"], srt)[0]
            edit_url = telegra_ph.publish2telegraph(
                config["telegra.ph"]["access_token"], video["title"], paragraphs)[0]

            tg_message = f'<b>{video["channel_name"]}\n</b>' \
                         + f'<u>{video["title"]}\n</u>' \
                         + f'👉<a href="{srt_url}" >字幕(subtitle)</a>' \
                         + f'👉<a href="{edit_url}">全文(fulltext)</a>\n' \
                         + result

            res = send_telegram_message(
                config["telegram_bot"]["token"], video["tg_user_id"], tg_message)
            if res.status != 200:
                print(
                    f"Error: telegram message sent failed! status_code={res.status}, text={res.text}")
                continue
            else:
                print(
                    f"video {video['title']} summerized and sent to user {video['tg_user_id']}!")

            # 将处理完的视频和相关数据保存到数据库
            conn.insert_data_to_database("user_video", tg_user_id=video['tg_user_id'], video_url=video['link'],
                                         channel_name=video['channel_name'], title=video['title'], srt_url=srt_url, edit_url=edit_url, result=result)

        if new_video_time > 0:
            conn.update_data_to_database("user_channel", {"newest_video_time": new_video_time},
                                         {"tg_user_id": video["tg_user_id"], "channel_url": channel})


async def main():
    import aiosqlite
    import json
    import sqlite3
    import time
    import asyncio
    lock = asyncio.Lock()

    path = "config.json"
    with open(path, 'r') as f:
        config = json.load(f)
    video_pool = {}
    async with aiosqlite.connect(config['db_path']) as conn:
        conn.row_factory = sqlite3.Row  # return dict instead of tuple
        while True:
            await update_video_pool(conn, video_pool, lock)
    print(video_pool)
