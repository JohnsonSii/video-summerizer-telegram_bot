import openai
import pandas as pd
import tiktoken 
import os
import traceback

# def estimate_token_count(text, model):
#     encoding = tiktoken.encoding_for_model(model)
#     return len(encoding.encode(text))


class SrtSummarizer:
    def __init__(self, config ):

        self.init_openai(config)

        self.max_token_count = config["max_tokens"]    
        self.model=config["model"] 
        
        self.encoding = self.init_encoding(config["model"])

        self.config=config
    def init_openai(self, config):
        openai.api_key = config["key"]
        if config["api_base_url"]:
            openai.api_base = config["api_base_url"]
        if config['api_type']:
            openai.api_type = config['api_type']
        if config['api_version']:
            openai.api_version = config['api_version']

    def init_encoding(self, model):
        try:
            encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            print("Warning: model not found. Using cl100k_base encoding.")
            encoding = tiktoken.get_encoding("cl100k_base")
        return encoding

    def estimate_token_count(self, text):

        return len(self.encoding.encode(text))   

    def estimate_tokens_from_messages(self, messages):   
        # source: https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
        """Return the number of tokens used by a list of messages."""
        model=self.config["model"]
        
        if model in {
            "gpt-3.5-turbo-0613",
            "gpt-3.5-turbo-16k-0613",
            "gpt-4-0314",
            "gpt-4-32k-0314",
            "gpt-4-0613",
            "gpt-4-32k-0613",
            "gpt-35-turbo-0613-jpe",
            }:
            tokens_per_message = 3
            tokens_per_name = 1
        elif model == "gpt-3.5-turbo-0301":
            tokens_per_message = 4  # every message follows <|start|>{role/name}\n{content}<|end|>\n
            tokens_per_name = -1  # if there's a name, the role is omitted
        elif "gpt-3.5-turbo" in model:
            print("Warning: gpt-3.5-turbo may update over time. Returning num tokens assuming gpt-3.5-turbo-0613.")
            tokens_per_message = 3
            tokens_per_name = 1
        elif "gpt-4" in model:
            print("Warning: gpt-4 may update over time. Returning num tokens assuming gpt-4-0613.")
            tokens_per_message = 3
            tokens_per_name = 1
        else:
            raise NotImplementedError(
                f"""num_tokens_from_messages() is not implemented for model {model}. See https://github.com/openai/openai-python/blob/main/chatml.md for information on how messages are converted to tokens."""
            )
        num_tokens = 0
        for message in messages:
            num_tokens += tokens_per_message
            for key, value in message.items():
                num_tokens += len(self.encoding.encode(value))
                if key == "name":
                    num_tokens += tokens_per_name
        num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>

        return num_tokens


    def split_srt(self, srt):
        ## srt to segmentation
        token_count = self.max_token_count / 3  ## split srt into segments with 1000 tokens
        tmp = 0
        tmp_str = ''
        result = []
        for index, row in srt.iterrows():
            if not row['text'] :  ## skip empty lines
                continue
            tmp = tmp + self.estimate_token_count( row['text'])
            tmp_str = tmp_str + ',' + row['text']
            if tmp > token_count:
                result.append([tmp, tmp_str])
                tmp_str = ''
                tmp = 0
        
        result.append([tmp, tmp_str])  ## last segment

        return result

    def edit_segment(self, segment):
        prompt = self.config["roles"]["editor"]["prompt"]

        temperature=self.config["roles"]["editor"]["temperature"]

        message=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": segment}
                ]
        message_token_count=self.estimate_tokens_from_messages(message)

        payload = {
            'messages': message,
            #'max_tokens': message_token_count,
            'temperature': temperature,
            'engine': self.config["model"],
        }
        return self.get_assistant_reply(payload)

    def summerize_segment(self, segment, output_token_count):
        prompt = self.config["roles"]["summerizer"]["prompt"]

        temperature=self.config["roles"]["summerizer"]["temperature"]
        messages=[{"role": "system", "content": prompt},
                {"role": "user", "content": segment}]
        
        payload = {
            'messages': messages,
            #'max_tokens': output_token_count,  
            'temperature': temperature,
            'engine': self.config["model"],
        }
        return self.get_assistant_reply(payload)
    

    def get_assistant_reply(self, payload):
        for i in range(5):
            try:
                response = openai.ChatCompletion.create(**payload)
        # 提取助手的回答
                assistant_reply = response['choices'][0]['message']['content']
                return assistant_reply
            except:   ##打印出错信息
                traceback.print_exc()
                print(f"retrying get_assistant_reply, the {i+1} times ...")
        return ''

    def edit(self, srt):
        segments = self.split_srt(srt)
        paragraphs = []
        for seg in segments:
            paragraphs.append(self.edit_segment(seg[1])) ###
        return paragraphs

    def summarize(self, paragraphs):

        keypoints = []
        for seg in paragraphs:
            if not seg: # skip empty segments
                continue
            token_count=self.estimate_token_count(seg)  # output 1/5 tokens
            key=self.summerize_segment(seg, int(token_count/5)) 
            if key:
                keypoints.append(key)
            else:
                print("key is empty, skip this segment!")


        xx=len(keypoints) 
        if  xx>1:
            return self.merge_keypoints(keypoints)
        elif xx==1:
            return keypoints[0]
        else:
            return 

    def merge_keypoints(self, keypoints):
        tmp_str = '\n'.join(keypoints)
        token_count = self.estimate_token_count(tmp_str)   

        if token_count*1.3 > self.max_token_count:
            print('warning, the responses needed to merge are too long!', 'token_acount=', token_count)
            print('return simple merge of response from all segments')
            return '\n'.join(keypoints) 
        else:
            res = self.summerize_segment(tmp_str, self.max_token_count-token_count )
            return res


if __name__ == "__main__": 
    ### usage examples
    import time
    import json
    t0=time.time()

    srt=pd.read_csv('d:/abc.csv')
    path=r"C:\Users\liuqimin\video-summerizer-config.json"
    with open(path, 'r') as f:
        config=json.load(f)
        srt_summarize = SrtSummarizer(config["openai"], [] )
    os.environ["HTTP_PROXY"] = config["proxies"]["http"]
    os.environ["HTTPS_PROXY"] = config["proxies"]["https"]

    paragraphs=srt_summarize.edit(srt)
    result=srt_summarize.summarize(paragraphs)

    print(result)
    print(paragraphs)
    print("总共花了：",time.time()-t0)
