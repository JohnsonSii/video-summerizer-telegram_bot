import time
import utils
import os
import json
def split_paragraphs(paragraphs):
    result =  []
    content=[]
    for item in paragraphs:
        content.append({"tag":"p", "children":[f"{item}"]})
        str0=''.join(str(content))
        if len(str0.encode('utf-8'))>30*1000: # 64KB limitation, 40k bytes, equal not used, and not needed 
            result.append(content[:-1])
            content=[{"tag":"p", "children":[f"{item}"]}]
    result.append(content)

    return result

def get_random_string(length):
    import random
    import string
    letters = string.ascii_lowercase+string.digits
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def publish2telegraph(access_token, title, paragraphs):
    
    contents = split_paragraphs(paragraphs)    
    page_urls=[]
# for short srt only one page 
    if len(contents)==1:
        page_urls.append( create_telegraph_page(access_token, title, contents[0]) )
        return page_urls

# for long srt, generate multiple pages, 
    # 1.  first generate len(contents) empty pages
    
    print("1. first generate len(contents) empty pages")   
    for i in range(len(contents)):
        url=create_telegraph_page(access_token, get_random_string(10), ["This is my first content page!"])
        page_urls.append( url )
    nav=generate_page_navigation(page_urls)

    # 2.  edit each page  for better navigation      

    print("2.  edit each page  for better navigation     ")   
    for i in range(len(contents)):
        edit_telegraph_page(access_token, title, [nav] + contents[i] + [nav] , page_urls[i][18:] )
        
    return page_urls

## srt is a pandas dataframe, with columns: start, duration, text
def srt2paragraphs(srt):
    paragraphs =  []
    for index, row in srt.iterrows():
        t0=time.gmtime( int(row['start']) )
        xx=time.strftime("%H:%M:%S", t0) +"\t"
        paragraphs.append(xx+f"{row['text']}\n")
    return paragraphs

def publish_srt_to_telegraph(access_token, title, srt):
    paragraphs=srt2paragraphs(srt)
    return publish2telegraph(access_token,title , paragraphs)

def create_telegraph_page(access_token, title, content):
    base_url = "https://api.telegra.ph/createPage"
    params = {
        "access_token": access_token,  # 可选，如果你有一个
        "title": title,
        "content": json.dumps(content, ensure_ascii=False),
        "return_content": True
    }
    return post_telegraph_page(base_url, params)


# edit existing page
def edit_telegraph_page(access_token, title, content, path):
    base_url = "https://api.telegra.ph/editPage"   
    params = {
        "access_token": access_token,  # 可选，如果你有一个
        "title": title,
        "content": json.dumps(content, ensure_ascii=False),
        "path": path,  #  去除http://telegra.ph/后的路径
        "return_content": True
    }

    return post_telegraph_page(base_url, params)

##
def post_telegraph_page(base_url, params):

    json_response = json.loads(utils.get_http_responce(base_url,"POST",  params).data)
    if json_response.get("ok"):
        return json_response["result"]["url"] 
    else:
        print("Error:", json_response.get("error"))
        print(f"Generate the 1th page url failed") 
        exit(1)
        return None

def generate_page_navigation(page_urls): 
    result={'tag':'p', 'children':["Page list:\t"]}
    for index,url in enumerate(page_urls):
        tt={
            'tag':'a', 
            'attrs':{'href':url}, 
            'children':[f"Page {index+1}"]
            }
        result['children'].append(tt)  
        result['children'].append('|')  

    return result
