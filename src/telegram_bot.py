import logging
import asyncio
import time
import os
import json
import redis
import httpx
import pytube
from db_query import *
from aiogram import Bot, Dispatcher, html
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, Message, CallbackQuery
from aiogram.enums import ParseMode
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage


logging.basicConfig(level=logging.INFO)

CHECK_INTERVAL = 5
VIDEO_POOL_NAME = "video_pool"

class VideoStates(StatesGroup):
    ADD_CHANNEL = State()
    ADD_VIDEO = State()

class VideoBot:
    def __init__(self, bot_token, db_config, redis_config) -> None:
        self.bot_token = bot_token
        self.db_config = db_config
        self.redis_config = redis_config
        self.conn: MySQLClientConnection = MySQLClientConnection(db_config)
        self.redis_client = redis.Redis(
            host=redis_config['host'], port=redis_config['port'], decode_responses=True)

    async def set_menu(self):
        commands = [
            BotCommand(command='/add_channel', description='添加频道'),
            BotCommand(command='/view_channel', description='查看频道列表'),
            BotCommand(command='/remove_channel', description='删除频道'),
            BotCommand(command='/view_videos', description='我的视频'),
            BotCommand(command='/add_video', description='添加单条视频'),
            BotCommand(command='/readme', description='Read me')
        ]
        await self.bot.set_my_commands(commands)

    async def add_user_if_not_exist(self, user_id, username):
        user = self.conn.select_data_from_database("user", tg_user_id=user_id)
        if not user:
            now = time.time() + time.altzone
            self.conn.insert_data_to_database("user", tg_user_id=user_id, username=username,
                                              last_processed_video_time=now)

    async def readme(self, message: Message):
        tt = """
        This bot is a video summarizer. 
        It will try to get all the newest video in the channel list (only YouTube channel at present) and then try to generate subtitles and then summarize the subtitles using ChatGPT.
        It will help you get the main idea of the video without watching the video.
        The bot is still under development, so it may not work properly.
        """
        await message.answer(tt)

    async def add_channel(self, message: Message, state: FSMContext):
        user_id = message.from_user.id
        username = message.from_user.full_name
        await self.add_user_if_not_exist(user_id, username)
        await state.set_state(VideoStates.ADD_CHANNEL)
        await message.answer("Please input the URL of a video, the bot will detect the channel automatically:")

    async def add_video(self, message: Message, state: FSMContext):
        user_id = message.from_user.id
        username = message.from_user.full_name
        await self.add_user_if_not_exist(user_id, username)
        await state.set_state(VideoStates.ADD_VIDEO)
        await message.answer("Please input the URL of a video, the bot will send the video to the priority queue, and once it is processed, it will automatically push it to you.")

    async def view_channel(self, message: Message):
        user_id = message.from_user.id
        username = message.from_user.full_name
        await self.add_user_if_not_exist(user_id, username)
        all_channel = self.conn.select_data_from_database("user_channel", tg_user_id=user_id)
        if not all_channel:
            await message.answer('No channel added yet.')
        else:
            tt = 'All channels:\n'
            for index, item in enumerate(all_channel):
                tt += f"{index + 1}. {item['channel_name']}\n"
            await message.answer(tt)

    async def view_videos(self, message: Message):
        user_id = message.from_user.id
        username = message.from_user.full_name
        await self.add_user_if_not_exist(user_id, username)
        keyboard = []
        my_videos = self.conn.select_data_from_database("user_video", tg_user_id=user_id)
        for item in my_videos:
            keyboard.append([InlineKeyboardButton(text=item['title'], callback_data=f"view:{item['video_link']}")])
        keyboard.append([InlineKeyboardButton(text='清空历史记录', callback_data="clear:clear")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        await message.answer('View detailed information on the following videos:\n', reply_markup=reply_markup)


    async def callback_handler(self, query: CallbackQuery):
        user_id = query.from_user.id
        username = query.from_user.full_name
        await self.add_user_if_not_exist(user_id, username)
        action, data = query.data.split(':', 1)
        if action == 'remove':
            channel = self.conn.select_data_from_database("user_channel", tg_user_id=user_id, channel_url=data)
            if channel:
                self.conn.delete_data_from_database("user_channel", tg_user_id=user_id, channel_url=data)
                await query.message.edit_text(f'Channel: {channel[0]["channel_name"]} removed!')
            else:
                await query.message.edit_text(f'Channel not found: {data}')
        elif action == 'view':
            videos = self.conn.select_data_from_database("video", video_url=data)
            if videos:
                video = videos[0]
                srt_url = video['srt_url']
                edit_url = video['edit_url']
                result = video['result']
                tg_message = f'<b>{video["channel_name"]}\n</b><u>{video["title"]}\n</u>' \
                             f'👉<a href="{srt_url}" >字幕(subtitle)</a>' \
                             f'👉<a href="{edit_url}">全文(fulltext)</a>\n' \
                             + result
                await query.message.edit_text(tg_message, parse_mode='HTML')
        elif action == 'clear':
            self.conn.delete_data_from_database("user_video", tg_user_id=user_id)
            await query.message.edit_text(f'Successfully cleared history!')
        await query.answer()

    async def remove_channel(self, message: Message):
        user_id = message.from_user.id
        username = message.from_user.full_name
        await self.add_user_if_not_exist(user_id, username)
        keyboard = []
        all_channel = self.conn.select_data_from_database("user_channel", tg_user_id=user_id)
        for item in all_channel:
            keyboard.append([InlineKeyboardButton(text=item['channel_name'], callback_data=f"remove:{item['channel_url']}")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        await message.answer('Select channel to remove:\n', reply_markup=reply_markup)

    def get_youtube_channel(self, url):
        proxies = {
            "http": os.environ["HTTP_PROXY"],
            "https": os.environ["HTTPS_PROXY"]
        }
        try:
            x = pytube.YouTube(url, proxies=proxies)
            channel_id = x.channel_id
            channel_name = x.author
            title, watch_url = x.title, x.watch_url
            return {'url': f"youtube/channel/{channel_id}", "name": channel_name, "id": channel_id, "watch_url": watch_url, "title": title}
        except Exception as e:
            print(f'Failed to get channel from {url}: {e}')
            return False

    async def message_handler(self, message: Message, state: FSMContext):
        user_id = message.from_user.id
        username = message.from_user.full_name
        await self.add_user_if_not_exist(user_id, username)
        url = message.text
        current_state = await state.get_state()
        if current_state == VideoStates.ADD_CHANNEL.state:
            channel = self.get_youtube_channel(url)
            if channel:
                ch = self.conn.select_data_from_database("user_channel", tg_user_id=user_id, channel_url=channel['url'])
                if ch:
                    await message.answer(f"Channel: {channel['name']} already exists!")
                else:
                    self.conn.insert_data_to_database('user_channel', tg_user_id=user_id,
                                                      channel_url=channel['url'], channel_name=channel['name'], newest_video_time=time.time() + time.altzone)
                    await message.answer(f"Channel: {channel['name']} added!")
            else:
                await message.answer(f'{url} is not a valid YouTube URL!')
            await state.clear()
        elif current_state == VideoStates.ADD_VIDEO.state:

            channel = self.get_youtube_channel(url)

            if not channel:
                await message.answer("This is not a valid YouTube video URL, Please try again!")
                return

            video_data = {
                "title": channel['title'],
                "link": channel['watch_url'],
                "tg_user_id": user_id,
                "channel_name": channel['name'],
                "channel_url": url
            }

            # print(video_data)

            priority_queue_key = "priority_queue"
            existing_videos = self.redis_client.hget(VIDEO_POOL_NAME, priority_queue_key)

            if existing_videos:
                old_videos = json.loads(existing_videos)
                old_videos.append(video_data)
            else:
                old_videos = [video_data]

            self.redis_client.hset(VIDEO_POOL_NAME, priority_queue_key, json.dumps(old_videos))

            await message.answer('✅ The video has entered the queue. Please be patient and wait!')
            await state.clear()

    async def run(self):
        print("Configuration of proxy is:", os.environ["HTTP_PROXY"])
        session = AiohttpSession(proxy=os.environ["HTTP_PROXY"])
        self.bot = Bot(token=self.bot_token, session=session, parse_mode=ParseMode.HTML)
        dp = Dispatcher(storage=MemoryStorage())

        await self.set_menu()

        dp.message.register(self.readme, Command(commands=["readme"]))
        dp.message.register(self.add_channel, Command(commands=["add_channel"]))
        dp.message.register(self.view_channel, Command(commands=["view_channel"]))
        dp.message.register(self.remove_channel, Command(commands=["remove_channel"]))
        dp.message.register(self.view_videos, Command(commands=["view_videos"]))
        dp.message.register(self.add_video, Command(commands=["add_video"]))
        dp.message.register(self.message_handler)
        dp.callback_query.register(self.callback_handler)

        await dp.start_polling(self.bot)
        await session.close()
