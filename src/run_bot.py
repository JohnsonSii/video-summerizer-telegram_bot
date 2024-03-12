from telegram_bot import Bot
import json
import os
import argparse

parser = argparse.ArgumentParser(
    prog='RunBot',
    description='Telegram bot for video summarization')

parser.add_argument('-c', '--config', type=str)
args = parser.parse_args()

if __name__ == "__main__":
    path = args.config

    # read config file with encoding detection
    config = json.load(open(path, 'r', encoding='utf-8'))

    os.environ["HTTP_PROXY"] = config["proxies"]["http"]
    os.environ["HTTPS_PROXY"] = config["proxies"]["https"]
    bot = Bot(config["telegram_bot"]['token'], config['mysql_info'], config['redis_info'])

    bot.run()
