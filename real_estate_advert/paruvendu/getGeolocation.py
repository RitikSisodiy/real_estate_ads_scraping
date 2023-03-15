from requests_html import HTMLSession,AsyncHTMLSession
import asyncio,json,os
import re,time,random
cpath =os.path.dirname(__file__) or "."
headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"
    }

class Fetch:
    def __init__(self) -> None:
        self.proxies = []
        self.lastproxycheck = time.time()-301
    def getProxy(self):
        if self.lastproxycheck and time.time()-self.lastproxycheck>300:
            filepath = f"{cpath}/working.txt"
            with open(filepath,"r") as file:
                proxies = file.readlines()
            self.lastproxycheck = time.time()
            self.proxies = [json.loads(proxy) for proxy in proxies]
        return self.proxies and random.choice(self.proxies)
    async def getGeo(self,res):
        try:
            id = res["id"]
            url = 'https://www.paruvendu.fr/communfo/popincommunfo/default/popinajax?show_html=1&openmap=1&type=googlemap&json={"idannonce":'+str(id)+'}'
            # url = 'https://www.paruvendu.fr/communfo/popincommunfo/default/popinajax?show_html=1&openmap=1&type=googlemap&json
            # try:
            r = await self.fetch(url)
            # except:
            #     return await self.getGeo(res)
            text =r.html.find("script")[-1].text
            pattern = r"(?<=myLngLat = \[)(?:.*?)(?=\];)"
            result = re.search(pattern,text)
            if result:
                res["latitude"], res["longitude"] = result.group(0).strip().split(", ")[::-1]
                res["location"] = f'{res["latitude"]}, {res["longitude"]}'
            return res
        except:
            return res
    async def fetch(self,url,retry=0):
        if retry>10:
            return 
        retry +=1
        try:
            r = await self.session.get(url,headers=headers,timeout=5)
            return r
        except:
            self.session.proxies = self.getProxy()
            return await self.fetch(url,retry)
    async def getAllgeo(self,ids,proxy = {}):
        tasks = []
        res = []
        self.session = AsyncHTMLSession()
        proxy =  proxy or self.getProxy()
        print(proxy)
        self.session.proxies =proxy
        for id in ids:
            tasks.append(asyncio.ensure_future(self.getGeo(id)))
            if len(tasks)>=100:
                res += await asyncio.gather(*tasks)
                tasks = []
        res += await asyncio.gather(*tasks)
        await self.session.close()
        return res
# Set up the initial search query

if __name__=="__main__":
    data = json.load(open("out.json"))
    data= data[:10]
    print(len(data))
    ob = Fetch()
    res = asyncio.run(ob.getAllgeo(data))
    with open("our2.json",'w') as file:
        file.write(json.dumps(res))