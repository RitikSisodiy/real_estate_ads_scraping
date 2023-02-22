import requests,json
from urllib.parse import urlencode
import aiohttp,asyncio,dotenv,os
from aiosocksy.connector import ProxyConnector, ProxyClientRequest
import time
dotenv.load_dotenv()
proxy = os.getenv("Proxy")
# proxy = None
proxy = {'http': proxy,'https': proxy}


headersinfo = json.load(open("headers.json"))

class Status:
    token = {
            "token":"",
            "expiry":0
        }
    def __init__(self) -> None:
        connector = ProxyConnector()
        self.session = aiohttp.ClientSession(connector=connector, request_class=ProxyClientRequest)
    async def getToken(self):
        if Status.token and Status.token.get("expiry") and Status.token.get("expiry") - time.time()<300:
            return Status.token.get('token')
        headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "SeLoger/6.8.5 Dalvik/2.1.0 (Linux; U; Android 8.1.0; ASUS_X00TD Build/OPM1)",
                "Accept": "application/json",
                "Host": "api-seloger.svc.groupe-seloger.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
            }
        SELOGER_SECURITY_URL = "https://api-seloger.svc.groupe-seloger.com/api/security"
        time_token = await self.session.get(f"{SELOGER_SECURITY_URL}/register", headers=headers,proxy=proxy["http"])
        time_token = await time_token.json()
        challenge_url = f"http://127.0.0.1:8001/seloger-auth?{urlencode(time_token, doseq=False)}"
        token = await self.session.get(challenge_url)
        token = await token.text()
        print(token,"self genrager troe")
        res = await self.session.get(f"{SELOGER_SECURITY_URL}/challenge",headers={**headers, **{'authorization': f'Bearer {token}'}},proxy=proxy["http"])
        assert res.status ==200
        final_token = (await res.text())[1:-1]
        Status.token["token"]  = final_token
        Status.token["expiry"] = time.time() + 300
        return final_token
    async def okhttp(self,url,headers={},proxies=None,params= {}):
        jsonbody = {
        "url":f"{url}?{urlencode(params)}",
        "headers":headers,
        "proxy":(proxies and (proxies.get("http") or proxies.get("https") )) or None
        }
        # print(jsonbody)
        r = await self.session.post("http://127.0.0.1:8001/makerequest",json=jsonbody)
        # print(r.headers)
        # r.url = url
        return r
    async def checkstatus(self,url="",id="",website="",retry=0):
        if retry>4:
            return 500
        info = headersinfo.get(website) or headersinfo.get("default")
        if info.get("statusurl"):
            url = info['statusurl'].replace("<id>",id)
        oid = url[url.rfind("/"):]
        headers,method = info["headers"],info["method"]
        if website=="seloger.com":
            headers["authorization"]= f'Bearer {await self.getToken()}'
        if method =="requests":
            r = await self.session.get(url,headers=headers,proxy=proxy["http"])
            status = r.status
        else:
            r = await self.okhttp(url,headers=headers,proxies=proxy)
            status = r.status
        if status == 403:pass
        else:
            if r.status ==200 and website=="pap.fr":
                res = await r.json()
                status = 200 if res["success"] else 404
                return status
            if r.status ==200 and website=="paruvendu.fr" and len(url.split("/"))<=5:
                if r.url.__str__() == "https://www.paruvendu.fr/":return 404
                else: return 200
            rurl = r.url.__str__()
            fid = rurl[rurl.rfind("/"):]
            print(status,rurl,url==rurl,fid,oid,fid==oid)
            if status ==200 and (url==rurl or fid==oid):
                return 200
            else:
                print(status)
                return 404
        print("retring")
        return  await self.checkstatus(url,website,retry+1)
async def main(url,website):
    ob = Status()
    r = await ob.checkstatus(url,website)
    await ob.session.close()
    return r 
if __name__=="__main__":
    import sys
    try:
        args = sys.argv
        _,url,website  = sys.argv
        for i in range(0,3) :
            r = asyncio.run(main(url,website))
            print(r)
    except Exception as e:
        print(e)
        print("enter url followed by website after command")
