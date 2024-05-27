import redis
from db_query import *
import time
from youtube2srt import audio2text, SubtitleDownloader
from summarizer import SrtSummarizer
import asyncio
import json
import telegra_ph
import utils


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


@utils.retry(retries=4, delay=1)
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
        raise ("Error: telegram message sent failed!")
        print(
            f"Error: telegram message sent failed! status_code={response.status}, text={response.text}")
        print("message:\n", message)
    return response


async def update_video_pool(conn, redis_client: redis.Redis, video_pool_name: str):
    print("Running update_video_pool")
    all_channels = conn.select_data_from_database("user_channel")
    print("all_channels:")
    for channel in all_channels:
        print("processing channel: ", channel["channel_name"])
        videos = await get_video_list(conn, channel)
        # print(channel["channel_name"], "contain:", videos)

        channel_url = channel["channel_url"]
        if videos:
            existing_videos = redis_client.hget(video_pool_name, channel_url)
            if existing_videos:
                old_videos = json.loads(existing_videos)
                old_videos.extend(videos)
            else:
                old_videos = videos
            redis_client.hset(video_pool_name, channel_url, json.dumps(old_videos))

    # print(redis_client.hgetall(video_pool_name))
    return


async def get_video_list(conn, channel):
    # user_channel=await select_data_from_database(conn, "user_channel", tg_user_id=channel["tg_user_id"], channel_url=channel["channel_url"])
    # user_channel=user_channel[0]

    old_video_time = channel["newest_video_time"]
    now = time.time() + time.altzone

    # 访问 rsshub 并解析 json，多次尝试，避免因网络请求不稳定而产生的问题
    @utils.retry(retries=4, delay=1)
    def rsshub_request(channel):
        try:
            res = utils.get_http_responce(
                "https://rsshub.app/" + channel["channel_url"] + "?format=json", 'GET', None)

            data = json.loads(res.data)
            return data
        except Exception as e:
            raise e

    data = rsshub_request(channel)
    result = []

    if data is None:
        return result

    for item in data["items"]:
        pubDate = item["date_published"]
        time_tuple = time.strptime(pubDate, "%Y-%m-%dT%H:%M:%S.%fZ")
        t1 = time.mktime(time_tuple)

        if t1 <= old_video_time:  # only process video published after last processed video
            break
        if now - t1 > 3600 * 24:  # only process video published in 1 day
            break

        print(f"channel {channel['channel_name']}: ",
              f'New video {item["title"]} found, adding to video pool!')
        result.append({"title": item["title"], "link": item["url"], "pubDate": t1, "tg_user_id": channel["tg_user_id"],
                       "channel_name": channel["channel_name"],
                       "channel_url": channel["channel_url"]})  # use timestamp as pubDate

    return result


def video_pool_is_empty(video_pool):
    x = False
    for channel in video_pool:
        x = x or json.loads(video_pool[channel])
    return not x


@utils.retry(retries=2, delay=1)
async def video_analysis(conn, config, downloader, srt_summarize, video):
    try:
        srt = downloader.get_subtitles(video["link"])
        if srt is None:
            raise ValueError(f"Subtitles could not be retrieved for video: {video['link']}")

        paragraphs = srt_summarize.edit(srt)
        result = srt_summarize.summarize(paragraphs)
        srt_url = telegra_ph.publish_srt_to_telegraph(
            config["telegra.ph"]["access_token"], video["title"], srt)[0]
        edit_url = telegra_ph.publish2telegraph(
            config["telegra.ph"]["access_token"], video["title"], paragraphs)[0]

        if not conn.select_data_from_database("video", video_url=video['link']):
            conn.insert_data_to_database("video", video_url=video['link'], channel_name=video['channel_name'],
                                         title=video['title'], srt_url=srt_url, edit_url=edit_url, result=result)

        tg_message = f'<b>{video["channel_name"]}\n</b>' \
                     + f'<u>{video["title"]}\n</u>' \
                     + f'👉<a href="{srt_url}" >字幕(subtitle)</a>' \
                     + f'👉<a href="{edit_url}">全文(fulltext)</a>\n' \
                     + result

        return tg_message
    except:
        return None


async def priority_videos_process(redis_client, video_pool, video_pool_name, channel, conn, config, downloader,
                                  srt_summarize):
    videos = json.loads(video_pool[channel])
    while videos:
        video = videos.pop(0)
        redis_client.hset(video_pool_name, channel, json.dumps(videos))

        video0 = conn.select_data_from_database("video", video_url=video["link"])
        if video0:
            video0 = video0[0]
            tg_message = f'<b>{video0["channel_name"]}\n</b>' \
                         + f'<u>{video0["title"]}\n</u>' \
                         + f'👉<a href=\"{video0["srt_url"]}\" >字幕(subtitle)</a>' \
                         + f'👉<a href=\"{video0["edit_url"]}\">全文(fulltext)</a>\n' \
                         + video0["result"]
        else:
            tg_message = await video_analysis(conn, config, downloader, srt_summarize, video)

        if tg_message is None:
            continue

        if not conn.select_data_from_database("user_video", tg_user_id=video["tg_user_id"], video_link=video["link"]):
            conn.insert_data_to_database("user_video", tg_user_id=video["tg_user_id"], video_link=video["link"],
                                         title=video["title"])

        res = send_telegram_message(config["telegram_bot"]["token"], video["tg_user_id"], tg_message)
        if res.status != 200:
            print(f"Error: telegram message sent failed! status_code={res.status}, text={res.text}")
            continue
        else:
            print(f"video {video['title']} summarized and sent to user {video['tg_user_id']}!")


async def video_summerizer(conn, config, redis_client: redis.Redis, video_pool_name: str):
    video_pool = redis_client.hgetall(video_pool_name)
    priority_queue_key = "priority_queue"
    if not redis_client.hget(video_pool_name, priority_queue_key):
        redis_client.hset(video_pool_name, priority_queue_key, json.dumps([]))

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
        if channel == priority_queue_key:
            await priority_videos_process(redis_client, video_pool, video_pool_name, channel, conn, config, downloader,
                                          srt_summarize)
            continue

        videos = json.loads(video_pool[channel])
        print(f"start summerize channel {channel}")

        if not videos:
            print(f"video pool for channel {channel} is empty!")
            continue

        new_video_time = -1
        while videos:
            video = videos.pop(0)
            redis_client.hset(video_pool_name, channel, json.dumps(videos))

            video0 = conn.select_data_from_database("channel_video", channel_url=channel, video_link=video["link"])
            if video0:
                video0 = conn.select_data_from_database("video", video_url=video["link"])[0]
                tg_message = f'<b>{video0["channel_name"]}\n</b>' \
                             + f'<u>{video0["title"]}\n</u>' \
                             + f'👉<a href=\"{video0["srt_url"]}\" >字幕(subtitle)</a>' \
                             + f'👉<a href=\"{video0["edit_url"]}\">全文(fulltext)</a>\n' \
                             + video0["result"]
            else:
                tg_message = await video_analysis(conn, config, downloader, srt_summarize, video)

            if tg_message is None:
                continue

            if not conn.select_data_from_database("channel_video", channel_url=video["channel_url"],
                                                  video_link=video["link"]):
                conn.insert_data_to_database("channel_video", channel_url=video["channel_url"],
                                             channel_name=video["channel_name"],
                                             video_link=video["link"])

            if not conn.select_data_from_database("user_video", tg_user_id=video["tg_user_id"],
                                                  video_link=video["link"]):
                conn.insert_data_to_database("user_video", tg_user_id=video["tg_user_id"], video_link=video["link"],
                                             title=video["title"])

            if video["pubDate"] > new_video_time:
                new_video_time = video["pubDate"]

            res = send_telegram_message(config["telegram_bot"]["token"], video["tg_user_id"], tg_message)
            if res.status != 200:
                print(f"Error: telegram message sent failed! status_code={res.status}, text={res.text}")
                continue
            else:
                print(f"video {video['title']} summarized and sent to user {video['tg_user_id']}!")

        if new_video_time > 0:
            conn.update_data_to_database("user_channel", {"newest_video_time": new_video_time},
                                         {"channel_url": channel})
