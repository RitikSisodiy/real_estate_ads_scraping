import asyncio
import fake_useragent
import datetime
import traceback
import aiofiles
import os
cpath =os.path.dirname(__file__) 

# from capachasolver_v2 import solveCapcha
with open(f"{cpath}/cookies.json",'r') as file:
    allcookie = file.read().splitlines() 
cookie =allcookie
count=0
lastupdate= None
updating = False
def updataStatus(status):
    with open("status.txt",'w') as file:
        file.write(status)
ua = fake_useragent.UserAgent(fallback='Your favorite Browser')
def getUserAgent():
    return ua.random
async def fetch(url,session,Json=False,file=False,**kwargs):
    global cookie
    global count
    global updating
    global lastupdate
    
    print("lenth of cooke +++++++++++++++++++++++", len(cookie))
    if not cookie:
        await asyncio.sleep(20)
        cookie = allcookie
    if not kwargs.get('headers'):
        kwargs["headers"] = {
                "user-agent":getUserAgent(),
                }
        try:
            kwargs["headers"]['cookie'] = cookie[count]
        except:
            count = 0
            kwargs["headers"]['cookie'] = cookie[count]
    try:
        # async with session.get(url,**kwargs) as response:
            if file:
                filename = str(datetime.datetime.now().date()) + '_' + str(datetime.datetime.now().time()).replace(':', '.')
                path = "avendrealouerImg/"
                filepath = path+filename
                f = await aiofiles.open(filepath, mode='wb')
                await f.write(await response.read())
                await f.close()
                return filepath
            try:
                # response = await session.get(url,**kwargs,proxies={'https': 'socks4://80.81.232.145:5678'})
                # response = await session.get(url,**kwargs,proxies={'https': 'socks4://127.0.0.1:9050'})
                response = await session.get(url,**kwargs)
            except:
                await asyncio.sleep(2)
                return await fetch(url,session,Json,file,**kwargs)
            # print()
            # response = await session.get(url,proxies={'https': 'socks4://5.58.66.55:14888'})
            # assert response.status == 200
            # print(response)
            try:
                updataStatus("200")
                if response.status_code == 500:
                    return None
                if response.status_code == 200:
                    if Json:
                        return  response.json()
                    # doc = await response.text()
                    # input('fetch input')
                    # html = HTML(html=doc)
                    # status =  response.status
                    print(url,response.status_code)
                    html = response.html
                    return html
                else:
                    print(response.status_code,"in else fetch")
                    print(url,"in else fetch")
                    updataStatus("403")
                    # input('continue')
                    count +=1
                    await asyncio.sleep(10)
                    with open(f"{cpath}/cookies.json",'r') as file:
                        cookie = file.read().splitlines()
                    
                    # time.sleep(5)
                    return await fetch(url,session,Json)
                    
                    # cookie = input("enter the cookies")
                    # html = await fetch(session,url)
                    # print(response.status)
                    # with open("errorpagesfinal.txt",'a') as file:
                    #     file.write(url+"\n")
                    # return html
            except Exception as e:
                print(response)
                print(traceback.format_exc())
                print(e,"got some isssue1")
                # print(e)
                input("some issue with that")
    except Exception as e:
        print(e,"got some isssue2")
        print(traceback.format_exc())
        pass