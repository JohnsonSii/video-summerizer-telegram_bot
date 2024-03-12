import youtube_dl
import  utils
import json
import pandas as pd
from whisper_helper import audio2text
import os

class SubtitleDownloader:
    def __init__(self, ydl_opts, audio2text_tool):
        ydl_opts["proxy"]=os.environ["HTTPS_PROXY"]
        self.ydl=youtube_dl.YoutubeDL(ydl_opts)
        self.audio2text_tool=audio2text_tool
    def download_audio(self, url):
        try:
            self.ydl.download([url])
        except youtube_dl.utils.DownloadError as e:
            print("DownloadError: ", e)
            return None


    def json2srt(self, subtitle_json):
        result = []

        for index, item in enumerate(subtitle_json['events']):
            start=item["tStartMs"]/1000
            duration= item["dDurationMs"]/1000
            text=item['segs'][0]['utf8']
            tmp = pd.DataFrame({'start': [start], 'duration': [duration], 'text': [text]}, index=[index])
            result.append(tmp)
        return pd.concat(result, axis=0)

    def get_subtitles(self, url):
        try:
            self.info_dict = self.ydl.extract_info(url, download=False)
            if self.info_dict.get('is_live'):
                print("live video, skip: ", url)
                return None
        except youtube_dl.utils.DownloadError as e:
            if "This live event will begin" in str(e):
                return None
            
        fname=self.ydl.prepare_filename(self.info_dict)
        if 'subtitles' in self.info_dict:   ##   video has subtitles
            subtitle=self.down_subtitle(self.info_dict['subtitles'])
            return  subtitle
        else:           ##video do not have subtitles
            
            self.download_audio(url)
            print(f" {fname} downloaded!")
            if(not os.path.exists(fname)):
                print(f" {fname} download failed!")
                return None
            subtitle=self.audio2text_tool.process(fname)

            return  subtitle


    def down_subtitle(self, subtitles):
        results={}
        langs=["zh-TW", "zh-Hans", "zh"]
         # 遍历每种语言的字幕，并下载到本地
        proxies={
            "http": os.environ["HTTP_PROXY"],
            "https": os.environ["HTTPS_PROXY"]
        }
        for lang in subtitles:   ## 暂时只下载一种字幕，中文或者英文
            if lang not in langs:
                continue
            if subtitles[lang][0]['ext']!='json3':
                print('json3 format subtitles not found')
                return None
            tmp= subtitles[lang][0]     # using json3 only
            tt=utils.get_http_responce(tmp['url'], 'GET', None)
            subtitle_json=json.loads(tt.data)
            subtitle=self.json2srt(subtitle_json)
            return subtitle

        return  results

if __name__ == "__main__":
    ## 语音转文本工具
    resolution=720
    ydl_opts={
        'format': f'bestaudio/best[height={resolution}]',
        'outtmpl': '%(title)s.%(ext)s',
    }
    model="large-v2"  #default large-v2
    gpu=0
    audio2text_tool=audio2text(model, gpu)

    # 创建字幕下载器实例

    downloader = SubtitleDownloader(ydl_opts, audio2text_tool)

    # 没有字幕的例子
    youtube_url = 'https://www.youtube.com/watch?v=R5gWsvgNiSU&ab_channel=%E8%BA%BA%E5%B9%B3%E5%8F%94%E5%B8%A6%E4%BD%A0%E4%BA%86%E8%A7%A3%E7%9C%9F%E6%AD%A3%E7%9A%84%E4%B8%AD%E5%9B%BD'
    srt2=downloader.get_subtitles(youtube_url)


    # 要下载字幕的YouTube视频链接
    # 有字幕的例子 zh-TW
    youtube_url = 'https://www.youtube.com/watch?v=LlcbISU2-UU&ab_channel=mrblock%E5%8D%80%E5%A1%8A%E5%85%88%E7%94%9F'
    srt1=downloader.get_subtitles(youtube_url)
    # zh-cn??
    youtube_url = 'https://www.youtube.com/watch?v=ZtRDVa_TccM&ab_channel=%E6%89%8B%E5%B7%A5%E8%80%BFHandyGeng'
    srt2=downloader.get_subtitles(youtube_url)




