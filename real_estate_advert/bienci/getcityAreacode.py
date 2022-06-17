from fetch import fetch
import asyncio
import json
from requests_html import AsyncHTMLSession
import random
citys = open("cities.json",'r').readlines()
citys = [json.loads(d)['pin'] for d in citys]
citys.sort()

async def main():
    tasks = []
    count = 0
    session = AsyncHTMLSession()
    for pin in citys:
        count+=1
        url = f"https://res.bienici.com/suggest.json?q={pin}&type=postalCode"
        tasks.append(asyncio.ensure_future(getDetails(session,url)))
        if len(tasks)==100 or count == len(citys):
            await asyncio.gather(*tasks)
            tasks= []
async def getDetails(session,url):
    r = await fetch(url,session,Json=True)
    if r:
        with open("finalcitys.json",'a') as file:
            file.write(json.dumps(r[0])+"\n")

asyncio.run(main())

