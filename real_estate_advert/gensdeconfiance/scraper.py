from datetime import datetime, timedelta
from HttpRequest.requestsModules import okHTTpClient
from HttpRequest.uploader import AsyncKafkaTopicProducer
import copy
import os, json
import rstr
import random
from datetime import datetime
import concurrent.futures
import os, json
import requests
import warnings, settings

warnings.filterwarnings("ignore", message="Unverified HTTPS request")
from saveLastChaeck import saveLastCheck
from .parser import ParseGensdeconfiance

kafkaTopicName = settings.KAFKA_GENSDECONFIANCE
commonTopicName = settings.KAFKA_COMMON_PATTERN
website = "gensdeconfiance.com"
commonIdUpdate = f"activeid-{website}"


class gensdeconfiance(okHTTpClient):
    """
    This class is used to crawl ads from gensdeconfiance.com.

    Attributes:
        apiurl: The API URL of gensdeconfiance.com.
        createurl: The URL to create a new account.
        loginurl: The URL to login to gensdeconfiance.com.
        params: A dictionary of parameters to be passed to the API.
        producer: An instance of AsyncKafkaTopicProducer.
        unchagedheaders: A dictionary of headers that do not change.
        cpath: The path of the current working directory.
    """

    def __init__(
        self,
        proxyThread=True,
        proxies={},
        aio=True,
        params={},
        asyncsize=1,
        cookies=True,
    ) -> None:
        self.apiurl = "https://gensdeconfiance.com/api/v2/ads"
        self.createurl = "https://gensdeconfiance.com/api/v2/members"
        self.loginurl = "https://gensdeconfiance.com/api/v2/members/login"
        cpath = os.path.dirname(__file__)
        URL = "https://gensdeconfiance.com/api/v2/ads?itemsPerPage=1"
        self.params = params
        proxyheaders = {}
        self.producer = AsyncKafkaTopicProducer()
        self.unchagedheaders = {
            "accept": "application/json",
            "cache-control": "no-cache",
            "appversion": "1.71.2.3200232",
            "apporigin": "android",
            "authorization": "undefined",
            "accept-language": "en-US",
            "Host": "gensdeconfiance.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "User-Agent": "okhttp/4.9.2",
            "uniquedeviceid": "96501a88a72976E6",
            "apptoken": "f9596bacf8e05070975e11df940a2084",
            "appvanity": "636e82f86000d",
        }
        super().__init__(
            proxyThread,
            URL,
            self.unchagedheaders,
            proxyheaders,
            proxies,
            aio,
            cpath,
            asyncsize,
            cookies=cookies,
        )

    def VerifyLogin(self):
        # Verifies if the user is logged in.
        r = self.fetch(f"{self.apiurl}?itemsPerPage=1")
        if not isinstance(r, list) and r.get("code") == 403:
            try:
                os.remove(f"{self.cpath}/account.json")
                os.remove(f"{self.cpath}/session.json")
            except:
                pass
            self.createAccount()
            self.login()

    def login(self):
        # Logs in to gensdeconfiance.com.
        try:
            with open(f"{self.cpath}/session.json", "r") as file:
                cred = json.load(file)
                self.headers[0].update(
                    {
                        "apptoken": cred["appToken"],
                        "appvanity": cred["vanity"],
                        "uniqueDeviceId": cred["uniqueDeviceId"],
                    }
                )
        except:
            try:
                with open(f"{self.cpath}/account.json", "r") as file:
                    cred = json.load(file)
                    print(cred)
                    self.headers[0]["uniquedeviceid"] = cred["uniqueDeviceId"]
                    try:
                        del self.headers[0]["apptoken"]
                        del self.headers[0]["appvanity"]
                    except:
                        pass
                    body = {
                        "email": cred["email"],
                        "password": cred["password"],
                        "uniqueDeviceId": cred["uniqueDeviceId"],
                    }
                    r = self.post(self.loginurl, body=body)
                    r = {**(r.json()["user"]), "uniqueDeviceId": cred["uniqueDeviceId"]}
                    with open(f"{self.cpath}/session.json", "w") as file:
                        file.write(json.dumps(r))
            except:
                self.createAccount()
                self.login()

    def init_headers(self, sid=0, init=False):
        # Initializes the headers.
        header = copy.deepcopy(self.unchagedheaders)
        self.session[sid].close()
        self.session[sid] = requests.Session()
        self.session[sid].verify = False
        try:
            self.proxy[sid] = (init and self.proxy.get(sid)) or self.getRandomProxy()
            cookie = self.proxy[sid]["cookies"]
            header["Cookie"] = cookie
            self.headers[sid] = header
        except Exception as e:
            print("in test4 except", e, sid)
            self.init_headers(sid)
        pass

    def getAdDetails(self, ad, sid=0):
        # Gets the details of an ad.
        r = self.fetch(f"{self.apiurl}/{ad['uuid']}", sid=sid)
        r = {
            **r,
            **ad,
        }
        return r

    def splitListInNpairs(self, li, interval):
        ran = len(li) / interval
        ran = int(ran) if ran == int(ran) else int(ran) + 1
        flist = []
        for i in range(0, ran):
            item = li[interval * i : interval * (i + 1)]
            flist.append(item)
        return flist

    def saveAdList(self, adsdata, onlyid=False):
        # Saves a list of ads.
        if onlyid:
            ads = [ParseGensdeconfiance(ad) for ad in adsdata]
            self.producer.PushDataList_v1(commonIdUpdate, ads)
        else:
            self.producer.PushDataList(kafkaTopicName, adsdata)
            parseAdList = [ParseGensdeconfiance(ad) for ad in adsdata]
            self.producer.PushDataList(commonTopicName, parseAdList)

    def save(self, data, getdata=False, onlyid=False):
        newads = []
        if onlyid:
            fetchedads = data
        else:
            fetchedads = []
            adlist = self.splitListInNpairs(data, self.asyncsize)
            for ads in adlist:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.asyncsize
                ) as excuter:
                    futures = excuter.map(
                        self.getAdDetails, ads, [i for i in range(0, len(ads))]
                    )
                    for f in futures:
                        fetchedads.append(f)
        self.saveAdList(fetchedads, onlyid=onlyid)

    def crawl(self, param={}, first=False, onlyid=False):
        # Crawl ads from gensdeconfiance.com.
        if not param:
            param = self.params
        response = self.fetch(self.apiurl, params=param)
        if first:
            return response[0]
        if not onlyid:
            self.createNewUpdate(latestad=response[0])
        while response:
            self.save(response, onlyid=onlyid)
            self.params["page"] += 1
            print(f"getting {self.params['page']}")
            response = self.fetch(self.apiurl, params=self.params)

    def getLastUpdate(self):
        try:
            with open(f"{self.cpath}/lastUpdate.json", "r") as file:
                data = json.load(file)
            return data
        except:
            return {}

    def getTimeStamp(self, strtime):
        formate = "%Y-%m-%dT%H:%M:%S+"
        t = datetime.strptime(strtime, formate)
        return t.timestamp()

    def getUpdateDicFromAd(self, ad):
        nowtime = datetime.now()
        upd = {
            "timestamp": nowtime.timestamp(),
            "lastupdate": ad.get("displayDate"),
        }
        return upd

    def ranmdombirthday(self):
        """
        This function will return a random datetime between two datetime
        objects.
        """
        start = datetime.strptime("1/1/1991 1:30 PM", "%m/%d/%Y %I:%M %p")
        end = datetime.strptime("1/1/2001 1:30 PM", "%m/%d/%Y %I:%M %p")
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + timedelta(seconds=random_second)

    def createAccount(self):
        # Creates a new account.
        deviceid = rstr.xeger(r"[0-9]{5}a[0-9]{2}a[0-9]{5}E[0-9]{1}")
        email = rstr.xeger(
            "[a-z]{"
            + f"{random.randint(3,7)}"
            + "}\.[a-z]{"
            + f"{random.randint(1,2)}"
            + "}[0-9]{"
            + f"{random.randint(3,7)}"
            + "}@gmail\.com"
        )
        password = rstr.xeger(r"[a-zA-z]{4}\.[a-z]{2}[0-9]{3}")
        fname = rstr.xeger("[a-z]{" + f"{random.randint(5,19)}" + "}")
        lname = rstr.xeger("[a-z]{" + f"{random.randint(5,19)}" + "}")
        self.headers[0]["uniquedeviceid"] = deviceid
        body = {
            "email": email,
            "locale": "en_US",
            "country": "auto",
            "uniqueDeviceId": deviceid,
            "firstName": fname,
            "lastName": lname,
            "birthdate": datetime.strftime(
                self.ranmdombirthday(), "%a, %d %b %Y %H:%M:%S"
            )
            + " GMT",
            "password": password,
        }
        r = self.post(self.createurl, body=body)
        d = {
            **r.json(),
            "email": email,
            "password": password,
            "uniqueDeviceId": deviceid,
        }
        with open(f"{self.cpath}/account.json", "w") as file:
            file.write(json.dumps(d))

    def createNewUpdate(self, latestad=None):
        if not latestad:
            latestad = self.getlatestAd()
        lastupd = self.getUpdateDicFromAd(latestad)
        with open(f"{self.cpath}/lastUpdate.json", "w") as file:
            file.write(json.dumps(lastupd))

    def getlatestAd(self):
        # Gets the latest ad.
        param = self.params
        param["orderColumn"], param["orderDirection"], param["itemsPerPage"] = (
            "displayDate",
            "orderDirection",
            1,
        )
        data = self.crawl(param=param, first=True)
        return data

    def updateLatestAd(self):
        updates = self.getLastUpdate()
        if updates:
            param = self.params
            param["orderColumn"], param["orderDirection"] = (
                "displayDate",
                "orderDirection",
            )
            updated = False
            first = True
            page = 1
            adcount = 0
            while not updated:
                param.update({"page": page})
                print(param)
                ads = self.fetch(self.apiurl, params=param)
                print(ads)
                updatedads = []
                updatetimestamp = updates["lastupdate"]
                for ad in ads:
                    adtimestamp = self.getUpdateDicFromAd(ad)["lastupdate"]
                    print(
                        f"   {adtimestamp}> {updatetimestamp}====>",
                        adtimestamp > updatetimestamp,
                    )
                    if adtimestamp > updatetimestamp:
                        if first:
                            self.createNewUpdate(ad)
                            first = False
                        updatedads.append(ad)
                        adcount += 1
                    else:
                        print(f"{adcount} new ads scraped ")
                        updated = True
                        break
                self.save(updatedads)
                page += 1
        else:
            print("there is no update available l")


params = {
    "category": "realestate",
    "orderColumn": "displayDate",
    "orderDirection": "DESC",
    "type": "offering",
    "rootLocales[]": "fr",
    "hidden": "true",
    "page": 1,
    "itemsPerPage": 50,
}


def main_scraper(payload, update=False):
    """
    Main function to process ads data based on the provided ads type.

    Args:
        payload: Dictionary containing types of ads to scrape(rental, sale, deleted etc)

    Returns:
        None
    """
    data = params
    adtype = payload.get("real_state_type")
    ob = gensdeconfiance(True, {}, True, data, 5)
    try:
        if adtype == "Updated/Latest Ads" or update:
            print(" latedst ads")
            ob.updateLatestAd()
        elif payload.get("real_state_type") == "deletedCheck":
            rescrapActiveId()
        else:
            ob.crawl()
    except:
        pass
    finally:
        ob.__del__()


def rescrapActiveIdbyType():
    data = params.copy()
    try:
        ob = gensdeconfiance(True, {}, True, data)
        ob.crawl(onlyid=True)
    finally:
        ob.__del__()


def rescrapActiveId():
    """
    Rescrapes active IDs and updates the last check time.

    Returns:
        None
    """
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    rescrapActiveIdbyType()
    saveLastCheck(website, nowtime.isoformat())
