from asyncio import protocols
from dataclasses import dataclass
import json
import asyncio
from requests_html import AsyncHTMLSession
import time
import fake_useragent
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
        session = AsyncHTMLSession()
        headers = kwargs.get("headers")
        if not headers:kwargs['headers']=self.headers
        print(url)
        res = await session.get(url,**kwargs)
        return res

    async def main(self):
        # self.session = AsyncHTMLSession()
        tasks = [asyncio.ensure_future(self.getProxy(url,protocol)) for url,protocol in self.urls]
        
        await asyncio.gather(*tasks)
        self.proxylist = []
        await asyncio.gather(asyncio.ensure_future(self.checkproxy()))
        return self.proxylist
    async def getProxy(self,url,protocol):
        global restext
        r = await self.fetch(url)
        content = r.content
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
                data = await asyncio.gather(*tasks)
                working = ""
                for d in data:
                    if d:
                        r, proxy = d
                        if r.status_code == 200:
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
            r= await self.fetch(url, proxies=proxies, timeout=1)
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
    SELOGER_SECURITY_URL = "https://api-seloger.svc.groupe-seloger.com/api/security/register"
    headers = {
                'User-Agent': 'okhttp/4.6.0',
            }
    ob = ProxyScraper(SELOGER_SECURITY_URL,headers)
    ob.FetchNGetProxy()
    ob.save()
    # asyncio.run(ob.main())
    # print(str(time.time()-stat)+" seconds")
