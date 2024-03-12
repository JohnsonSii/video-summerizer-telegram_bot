import json

import redis
from summarizer_helper import video_pool_update_task, video_summerizer_task
import asyncio
import time

from db_query import *
# import aiosqlite


# 每个任务建一个异步数据库连接更合适（连接数并不多）
# async def init_db(db_path):
#     db=await aiosqlite.connect(config['db_path'])
#     db.row_factory = aiosqlite.Row  ## return dict instead of tuple
#     return db


async def main():
    import os

    path = "../video-summerizer-config.json"

    # read config file with encoding detection
    config = json.load(open(path, 'r', encoding='utf-8'))

    redis_client = redis.Redis(
        host='localhost', port=6379, decode_responses=True)
    # redis_client.flushdb()
    video_pool_name = "video_pool"

    os.environ["HTTP_PROXY"] = config["proxies"]["http"]
    os.environ["HTTPS_PROXY"] = config["proxies"]["https"]
    os.environ["https_request_retry"] = str(config["https_request_retry"])

    # 1. video pool update task
    task0 = asyncio.create_task(
        video_pool_update_task(60 * 3, config, redis_client, video_pool_name))  # update video pool every half hour

    task1 = asyncio.create_task(video_summerizer_task(
        config, redis_client, video_pool_name))

    await asyncio.gather(task0, task1)


if __name__ == "__main__":
    asyncio.run(main())
