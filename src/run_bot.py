from telegram_bot import Bot
import json
import os

if __name__ == "__main__":
    # path = "../video-summerizer-config.json"
    path = r"C:\Users\Developer\Documents\Recent\video-summerizer-telegram_bot\video-summerizer-config.json"

    # read config file with encoding detection
    config = json.load(open(path, 'r', encoding='utf-8'))

    os.environ["HTTP_PROXY"] = config["proxies"]["http"]
    os.environ["HTTPS_PROXY"] = config["proxies"]["https"]
    bot = Bot(config["telegram_bot"]['token'], config['mysql_info'])

    bot.run()
