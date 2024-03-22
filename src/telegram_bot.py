import logging
import httpx
import redis

from telegram import __version__ as TG_VER
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, BotCommand
import asyncio
import pytube
from db_query import *
import time
# import aiosqlite
import os
import json

from telegram import __version_info__

CHECK_INTERVAL = 5

if __version_info__ < (20, 0, 0, "alpha", 5):
    raise RuntimeError(
        f"This example is not compatible with your current PTB version {TG_VER}. To view the "
        f"{TG_VER} version of this example, "
        f"visit https://docs.python-telegram-bot.org/en/v{TG_VER}/examples.html"
    )

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackContext,
    CallbackQueryHandler,
)

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


async def add_user_if_not_exist(conn: MySQLClientConnection, user_id, username):
    user = conn.select_data_from_database("user", tg_user_id=user_id)
    if not user:
        now = time.time() + time.altzone
        print("user not exists, adding user: ", username)
        conn.insert_data_to_database("user", tg_user_id=user_id, username=username,
                                     last_processed_video_time=now)
    return


class Bot:

    def __init__(self, bot_token, db_config, redis_config) -> None:
        # builder = ApplicationBuilder().token(bot_token)
        # self.application = builder.build()
        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(self.init_db(db_path))
        #
        # loop.run_until_complete(self.set_menu())
        # self.add_handlers()
        builder = ApplicationBuilder().token(bot_token)
        self.application = builder.build()
        self.conn: MySQLClientConnection = MySQLClientConnection(db_config)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.set_menu())
        self.add_handlers()
        self.redis_client = redis.Redis(
            host=redis_config['host'], port=redis_config['port'], decode_responses=True)
        self.video_pool_name = "video_pool"
        

    # async def init_db(self, db_path):
    #     self.conn = await aiosqlite.connect(db_path)
    #     self.conn.row_factory = aiosqlite.Row  ## return dict instead of tuple

    async def set_menu(self):
        commands = [
            BotCommand(command='/add_channel', description='添加频道'),
            BotCommand(command='/view_channel', description='查看频道列表'),
            BotCommand(command='/remove_channel', description='删除频道'),
            BotCommand(command='/view_videos', description='我的视频'),
            BotCommand(command='/add_video', description='添加单条视频'),
            BotCommand(command='/readme', description='Read me')
        ]
        await self.application.bot.setMyCommands(commands)

    def add_handlers(self):
        self.application.add_handler(CommandHandler("add_channel", self.add_channel))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.message_handler))
        self.application.add_handler(CommandHandler("readme", self.readme))
        self.application.add_handler(CommandHandler("view_channel", self.view_channel))
        self.application.add_handler(CommandHandler("remove_channel", self.remove_channel))
        self.application.add_handler(CommandHandler("view_videos", self.view_videos))
        self.application.add_handler(CommandHandler("add_video", self.add_video))
        self.application.add_handler(CallbackQueryHandler(self.callback_handler))
        self.application.add_error_handler(self.error_callback)

    async def readme(self, update: Update, context: CallbackContext) -> None:
        tt = """
    This bot is a video summerizer. 
    It will try to get all the newest video in the channel list (only youtube channel at present) and then try to generate subtitles and then summerize the subtitles using chatgpt.
    It will help you get the main idea of the video without watching the video.
    The bot is still under development, so it may not work properly.

    """
        await update.message.reply_text(tt)
        return

    async def add_channel(self, update: Update, context: CallbackContext) -> None:
        """Adds a channel to the list of channels the bot is in"""
        user_id = update.effective_user.id
        username = update.effective_user.full_name

        print("adding channel for ", username)

        await add_user_if_not_exist(self.conn, user_id, username)

        context.user_data['action'] = 'add_channel'

        await update.message.reply_text(
            f"please input the url of a video, the bot will detect the channel automaticly:")

    async def add_video(self, update: Update, context: CallbackContext) -> None:
        user_id = update.effective_user.id
        username = update.effective_user.full_name

        await add_user_if_not_exist(self.conn, user_id, username)

        context.user_data['action'] = 'add_video'

        await update.message.reply_text(
            f"Please input the url of a video, the bot will send the video to the priority queue, and once it is processed, it will automatically push it to you.")

    async def view_channel(self, update: Update, context: CallbackContext) -> None:
        """Adds a channel to the list of channels the bot is in"""
        user_id = update.effective_user.id
        username = update.effective_user.full_name
        await add_user_if_not_exist(self.conn, user_id, username)

        print("view channel for ", username)

        # all_channel=get_all_channel(self.cur, user_id)
        all_channel = self.conn.select_data_from_database(
            "user_channel", tg_user_id=user_id)

        context.user_data['action'] = 'view_channel'

        if not all_channel:
            await update.message.reply_text('no channel added yet')
        else:
            tt = 'all channels:\n'
            for index, item in enumerate(all_channel):
                tt = tt + str(index + 1) + ". " + item['channel_name'] + '\n'
            await update.message.reply_text(tt)

        return

    async def view_videos(self, update: Update, context: CallbackContext) -> None:
        user_id = update.effective_user.id
        username = update.effective_user.full_name
        await add_user_if_not_exist(self.conn, user_id, username)
        
        keyboard = []
        my_videos = self.conn.select_data_from_database(
            "user_video", tg_user_id=user_id)

        for item in my_videos:
            keyboard.append([InlineKeyboardButton(
                item['title'], callback_data=f"view:{item['video_url']}")])
            
        keyboard.append([InlineKeyboardButton(
            '清空历史记录', callback_data=f"clear:clear")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text('View detailed information on the following videos:\n', reply_markup=reply_markup)

    async def callback_handler(self, update: Update, context: CallbackContext) -> None:
        user_id = update.effective_user.id
        username = update.effective_user.full_name
        await add_user_if_not_exist(self.conn, user_id, username)

        print("remove channel for ", username)

        query = update.callback_query
        data = query.data

        # 分割 callback_data 来获取操作类型和数据
        action, data = data.split(':', 1)

        if action == 'remove':

            channel = self.conn.select_data_from_database(
                "user_channel", tg_user_id=user_id, channel_url=data)
            if channel:
                self.conn.delete_data_from_database(
                    "user_channel", tg_user_id=user_id, channel_url=data)
                await query.edit_message_text(text=f'channel:  {channel[0]["channel_name"]} removed!')
            else:
                await query.edit_message_text(text=f'channel not found: {data}')

        if action == 'view':
            videos = self.conn.select_data_from_database(
                "user_video", tg_user_id=user_id, video_url=data)
            if videos:
                video = videos[0]
                srt_url = video['srt_url']
                edit_url = video['edit_url']
                result = video['result']

                tg_message = f'<b>{video["channel_name"]}\n</b>' \
                    + f'<u>{video["title"]}\n</u>' \
                    + f'👉<a href="{srt_url}" >字幕(subtitle)</a>' \
                    + f'👉<a href="{edit_url}">全文(fulltext)</a>\n' \
                    + result

                await query.edit_message_text(text=tg_message, parse_mode='HTML')

        if action == 'clear':
            self.conn.delete_data_from_database(
                "user_video", tg_user_id=user_id)
            await query.edit_message_text(text=f'Successfully cleared history!')
        
        await query.answer()
        return

    async def remove_channel(self, update: Update, context: CallbackContext) -> None:

        user_id = update.effective_user.id
        username = update.effective_user.full_name
        await add_user_if_not_exist(self.conn, user_id, username)

        keyboard = []
        all_channel = self.conn.select_data_from_database(
            "user_channel", tg_user_id=user_id)

        for item in all_channel:
            keyboard.append([InlineKeyboardButton(
                item['channel_name'], callback_data=f"remove:{item['channel_url']}")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text('Select channel need to remove:\n', reply_markup=reply_markup)

    def get_youtube_channel(self, url):
        proxies = {
            "http": os.environ["HTTP_PROXY"],
            "https": os.environ["HTTPS_PROXY"]
        }
        try:
            x = pytube.YouTube(url, proxies=proxies)
            channel_id = x.channel_id
            channel_name = x.author
            return {'url': "youtube/channel/" + channel_id, "name": channel_name, "id": channel_id}
        except:
            print('get channel from ', url, 'failed!')
            return False

    async def message_handler(self, update: Update, context: CallbackContext) -> None:

        user_id = update.effective_user.id
        username = update.effective_user.full_name
        await add_user_if_not_exist(self.conn, user_id, username)

        url = update.message.text
        print('user_id ', user_id, 'url', url)

        if context.user_data.get('action') == 'add_channel':
            channel = self.get_youtube_channel(url)
            if channel:
                ch = self.conn.select_data_from_database("user_channel", tg_user_id=user_id,
                                                         channel_url=channel['url'])
                if ch:  # channel already exists
                    await update.message.reply_text(f"Channel: {channel['name']} already exists!")
                    return
                else:
                    self.conn.insert_data_to_database('user_channel', tg_user_id=user_id,
                                                      channel_url=channel['url'], channel_name=channel['name'], newest_video_time=time.time() + time.altzone)
                    await update.message.reply_text(f"Channel: {channel['name']} added!")
                    return
            else:
                await update.message.reply_text(f'{url} is not a valid youtube url!')

        elif context.user_data.get(
                'action') == 'view_channel':  # example of using context.user_data to pass data between handlers
            pass

        elif context.user_data.get('action') == 'add_video':
            proxies = {
                "http": os.environ["HTTP_PROXY"],
                "https": os.environ["HTTPS_PROXY"]
            }
            channel = self.get_youtube_channel(url)

            x = pytube.YouTube(url, proxies=proxies)
            title, watch_url = x.title, x.watch_url

            if self.redis_client.exists(channel['url']):
                old_videos = json.loads(self.redis_client.hget(
                    self.video_pool_name, channel['url']))
                old_videos.insert(0, {"title": title, "link": watch_url, "pubDate": time.time(
                ) + time.altzone, "tg_user_id": user_id, "channel_name": channel['name']})
                self.redis_client.hset(
                    self.video_pool_name, channel['url'], json.dumps(old_videos))
            else:
                self.redis_client.hset(
                    self.video_pool_name, channel['url'], json.dumps([{"title": title, "link": watch_url, "pubDate": time.time(
                    ) + time.altzone, "tg_user_id": user_id, "channel_name": channel['name']}]))

            await update.message.reply_text(f'The video has entered the queue. Please be patient and wait!')
        return

    def updater_is_running(self):
        return self.application.updater.running
    
    def restart_updater(self):
        self.application.updater.start_polling(allowed_updates=Update.ALL_TYPES, use_context=True)
    
    
    def error_callback(self, update, context):
        logger.warning('Update "%s" caused error "%s"', update, context.error)
        self.application.stop_running()
        self.application.run_polling(allowed_updates=Update.ALL_TYPES, poll_interval=0.1, connect_timeout=30.0)

        # 如果出现网络相关的错误，则尝试重新启动机器人
        if isinstance(context.error, httpx.RemoteProtocolError):
            logger.error("Attempting to restart the bot...")
            self.restart_bot()

    def restart_bot(self):
        # 停止机器人
        self.application.updater.stop()

        # 重新启动机器人
        logger.info("Restarting the bot...")
        self.run() 


    def run(self):
        # Run the bot until the user presses Ctrl-C
        self.application.run_polling(allowed_updates=Update.ALL_TYPES, poll_interval=0.1, connect_timeout=30.0)
        while True:
            try:
                asyncio.get_event_loop().run_until_complete(self.application.bot.get_me())
                # self.application.bot.get_me()
                time.sleep(CHECK_INTERVAL)
            except:
                self.application.stop_running()
                self.application.run_polling(allowed_updates=Update.ALL_TYPES, poll_interval=0.1, connect_timeout=30.0)
        