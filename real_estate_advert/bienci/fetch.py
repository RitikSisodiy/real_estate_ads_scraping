import asyncio
import time
import fake_useragent
import datetime
import traceback
import aiofiles
import os
cpath =os.path.dirname(__file__) or "." 

ua = fake_useragent.UserAgent(fallback='Your favorite Browser')
def getUserAgent():
    return ua.random
def fetch(url,session,Json=False,file=False,**kwargs):
    print(url)
    if not kwargs.get('headers'):
        kwargs["headers"] = {
                "user-agent":getUserAgent(),
                }
    try:
        # async with session.get(url,**kwargs) as response:
            try:
                response = session.fetch(url,**kwargs)
            except:
                traceback.print_exc()
                # await asyncio.sleep(2)
                time.sleep(1)
                return fetch(url,session,Json,file,**kwargs)
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
                    return fetch(url,session,Json)
            except Exception as e:
                # print(response)
                # print(traceback.format_exc())
                print(e,"got some isssue1")
                # print(e)
    except Exception as e:
        # print(e,"got some isssue2")
        # print(traceback.format_exc())
        pass