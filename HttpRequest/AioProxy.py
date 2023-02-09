from asyncio import protocols
from dataclasses import dataclass
import json
import asyncio,aiohttp
import time
import fake_useragent
from aiosocksy.connector import ProxyConnector, ProxyClientRequest
restext = []
ua = fake_useragent.UserAgent(fallback='Your favorite Browser')
def getUserAgent():
    return ua.random


class ProxyScraper:
    def __init__(self,url=None,headers=None) -> None:
        self.protocols = ['http','socks4','socks5']
        self.urls = []
        self.url = url
        self.headers = headers
        self.proxylist = []
        for protocol in self.protocols: self.urls.append([f"https://api.proxyscrape.com/v2/?request=getproxies&protocol={protocol}&timeout=10000&country=all&ssl=all&anonymity=all",protocol])
        pass
    async def fetch(self,url,**kwargs):
        # session = AsyncHTMLSession()
        connector = ProxyConnector()
        async with aiohttp.ClientSession(connector=connector, request_class=ProxyClientRequest) as session: 
            #headers = kwargs.get("headers")
            #if not headers:kwargs['headers']=self.headers
            print(url)
            res = await session.get(url,ssl=False,**kwargs)
            d =  res.status
            print(d)
            if kwargs.get("proxy"):
                pass
            else:
                d = await res.read()
            return d

    async def main(self):
        # self.session = AsyncHTMLSession()
        headers = {
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        }
        tasks = [asyncio.ensure_future(self.getProxy(url,protocol,headers)) for url,protocol in self.urls]
        
        await asyncio.gather(*tasks)
        self.proxylist = []
        await asyncio.gather(asyncio.ensure_future(self.checkproxy()))
        return self.proxylist
    async def getProxy(self,url,protocol,headers):
        global restext
        content = await self.fetch(url,headers=headers)
        # content = await r.read()
        with open(f"{protocol}.txt",'wb') as file:
            file.write(content)
        data = content.decode("utf-8")
        data = data.split("\r\n")    
        return True
    def FetchNGetProxy(self):
        self.proxylist = asyncio.run(self.main())
        return self.proxylist
    def GetProxyList(self):
        return self.proxylist
    def save(self,cpath=None):

        final = ""
        for proxy in self.proxylist:
            final  += json.dumps(proxy)+"\n"
        with open(f"{cpath}/working.txt",'w') as file:
            file.write(final)
    async def checkproxy(self):
        for protocol in self.protocols:
            with open(f"{protocol}.txt",'r') as file:
                proxies = file.readlines()
                tasks = []
                for proxy in proxies:
                    # curl = "https://google.com"
                    # curl = "https://www.cci.fr"
                    # curl = "https://www.meilleursagents.com/agence-immobiliere/"
                    # curl = "https://www.paruvendu.fr/immobilier/vente/parc-chambrun-temple-de-diane-produit-rare-06100/particulier/1256044856A1KIVHAP000"
                    # curl = "https://www.avendrealouer.fr"
                    proxy = proxy.replace('\n','')
                    tasks.append(asyncio.ensure_future(self.check_if_proxy_is_working(proxy,protocol,self.url)))
                    if (len(tasks)>=100):
                        data += await asyncio.gather(*tasks)
                        tasks =[]
                data += await asyncio.gather(*tasks)
                working = ""
                for d in data:
                    if d:
                        r, proxy = d
                        if r == 200:
                            working += json.dumps(proxy)+"\n"
                            # restext.append(proxy)
                            self.proxylist.append(proxy)
                print(working)
                # with open(f"working.txt",'a') as file1:
                #     file1.write(working)
                lent = {len(data)}
                print(f"{protocol} done {lent} proxies")


    async def check_if_proxy_is_working(self,proxies,protocol,url):
        proxies = {"http":f"{protocol}://{proxies}","https":f"{protocol}://{proxies}"}
        # url = f'{schema}://www.google.com/'
        # ip = proxies[schema].split(':')[1][2:]
        # print(proxies)
        try:
            # async with session.get(,) as r:
            #     return r
            print(proxies)
            r= await self.fetch(url, proxy=proxies["http"], headers=self.headers,timeout=10)
            print(r)
            return r,proxies
        except:
            pass
        return False

def getProxyasstring():
    stat = time.time()
    asyncio.run(main())
    return restext
if __name__ == '__main__':
    stat = time.time()
    # SELOGER_SECURITY_URL = "https://api-seloger.svc.groupe-seloger.com/api/security/register"
    LEBONCOIN_CHECK_URL = "https://api.leboncoin.fr/finder/classified/2130999715"
    headers = {
            'Accept-Language': "en-US,en;q=0.8,fr;q=0.6",
            'Accept-Encoding': "*",
            'User-Agent': "LBC;Android;11;sdk_gphone_x86;phone;8b1263fac1529be6;wifi;5.70.2;570200;0"
            }
    ob = ProxyScraper(LEBONCOIN_CHECK_URL,headers)
    ob.FetchNGetProxy()
    ob.save()
    # asyncio.run(ob.main())
    # print(str(time.time()-stat)+" seconds")
