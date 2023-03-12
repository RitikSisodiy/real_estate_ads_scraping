import asyncio
import time
import datetime
import traceback
import aiofiles
import os
cpath =os.path.dirname(__file__) or "." 

def fetch(url,session,Json=False,file=False,retry=0,**kwargs):
    print(url)
    if not kwargs.get('headers'):
        kwargs["headers"] = {
                "user-agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
                }
    if retry>=3:return
    retry +=1
    try:
        # async with session.get(url,**kwargs) as response:
            try:
                response = session.fetch(url,**kwargs)
            except:
                traceback.print_exc()
                # await asyncio.sleep(2)
                time.sleep(1)
                return fetch(url,session,Json,file,retry=retry,**kwargs)
            try:
                if response.status_code==404:
                    return {}
                if response.status_code == 500:
                    return None
                if response.status_code == 200:
                    if Json:
                        return  response.json()
                    # print(url,response.status_code)
                    html = response.html
                    return html
                else:
                    # await asyncio.sleep(1)
                    time.sleep(1)
                    return fetch(url,session,Json,file,retry=retry,**kwargs)
            except Exception as e:
                # print(response)
                # print(traceback.format_exc())
                print(e,"got some isssue1")
                # print(e)
    except Exception as e:
        # print(e,"got some isssue2")
        # print(traceback.format_exc())
        pass