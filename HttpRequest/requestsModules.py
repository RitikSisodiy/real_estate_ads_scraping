from .AioProxy import ProxyScraper as AioScraper
from .AsyncProxy import ProxyScraper as AsyncScraper
import threading, time, json, random, traceback, requests, os, concurrent.futures
from requests_html import AsyncHTMLSession
from urllib.parse import urlencode
from datetime import datetime


class ProxyServer:
    """
    Initialize the ProxyServer class.

    Args:
        proxyThread (bool): Flag indicating whether to run the proxy update thread.
        URL (str): URL for the server.
        headers (dict): Headers to be used in the HTTP requests.
        proxies (list): List of proxies to use.
        aio (bool): Flag indicating whether to use AioScraper or AsyncScraper.
        cpath (str): Path to store files.
        interval (int): Interval for updating the proxy list (in seconds).
        cookies (bool): Flag indicating whether to handle cookies or not.
    """

    def __init__(
        self, proxyThread, URL, headers, proxies, aio, cpath, interval, cookies
    ) -> None:
        self.logfile = open(f"{cpath}/error.log", "a")
        self.proxy = self.getLastProxy()
        print(aio, "this is aio")
        self.cpath = cpath
        self.interval = interval
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            }
        print("cookies", cookies)
        if aio:
            self.prox = AioScraper(URL, headers, cookies)
        else:
            self.prox = AsyncScraper(URL, headers)
        if not proxies:
            try:
                self.proxies = self.readProxy()
                if not self.proxies:
                    self.getProxyList()
            except:
                self.getProxyList()
        else:
            self.proxies = proxies
        self.proxy = self.getLastProxy()
        if proxyThread:
            self.proxyUpdateThread()

    def getProxyList(self):
        """
        Fetch and update the proxy list.
        """
        self.prox.FetchNGetProxy()
        self.prox.save(self.cpath)
        self.proxies = self.readProxy()

    def proxyUpdateThread(self):
        """
        Start the proxy update thread.
        """
        print("proxy thread is started")
        self.startThread = True
        self.proc = threading.Thread(target=self.updateProxyList, args=())
        self.proc.daemon = True
        self.proc.start()

    def threadsleep(self, t):
        """
        Sleep for a specified time.

        Args:
            t (int): Time in seconds to sleep.

        Returns:
            True if the thread is stopped, False otherwise.
        """
        while t > 0:
            if not self.startThread:
                return True
            time.sleep(1)
            t -= 1
        return False

    def updateProxyList(self):
        """
        Update the proxy list at a specified interval.
        """
        if self.readProxy() and self.threadsleep(self.interval):
            return
        while self.startThread:
            self.getProxyList()
            b = self.threadsleep(self.interval)
            if b:
                break
        print("thread is stopped")

    def getLastProxy(self):
        """
        Get the last working proxy from the file.

        Returns:
            The last working proxy as a dictionary.
        """
        try:
            with open(f"{self.cpath}/lastworking.json", "r") as file:
                d = json.load(file)
                d = {int(k): v for k, v in d.items()}
            return d
        except:
            return {}

    def __del__(self):
        """
        Destructor method.
        """
        self.startThread = False
        with open(f"{self.cpath}/lastworking.json", "w") as file:
            file.write(json.dumps(self.proxy))
        print("class destroyed")
        print("proxy thread is terminated")

    def readProxy(self):
        """
        Read the proxy list from the file.

        Returns:
            The list of proxies.
        """
        filepath = f"{self.cpath}/working.txt"
        lastmodifiled = os.stat(filepath).st_mtime
        ctime = datetime.now().timestamp()
        if ctime - lastmodifiled >= 60 * 60 * 24:
            return None
        with open(filepath, "r") as file:
            proxies = file.readlines()
        return [json.loads(proxy) for proxy in proxies]

    def getRandomProxy(self):
        """
        Get a random proxy from the proxy list.

        Returns:
            A random proxy.
        """
        proxy = random.choice(self.proxies)
        return proxy


class HttpRequest(ProxyServer):
    """
    Initialize the HttpRequest class.

    Args:
        proxyThread (bool): Flag indicating whether to run the proxy update thread.
        URL (str): URL for the server.
        headers (dict): Headers to be used in the HTTP requests.
        proxyheaders (dict): Headers specific to the proxy server.
        proxies (dict): Proxies to use.
        aio (bool): Flag indicating whether to use AioScraper or AsyncScraper.
        cpath (str): Path to store files.
        asyncsize (int): Number of asynchronous sessions to use.
        timeout (int): Timeout value for the requests.
        interval (int): Interval for updating the proxy list (in seconds).
        cookies (bool): Flag indicating whether to handle cookies or not.
        maxtry (bool): Flag indicating whether to allow maximum retries.
    """

    def __init__(
        self,
        proxyThread=True,
        URL="https://www.google.com",
        headers={},
        proxyheaders={},
        proxies={},
        aio=True,
        cpath="",
        asyncsize=1,
        timeout=5,
        interval=300,
        cookies=False,
        maxtry=False,
    ) -> None:
        super().__init__(
            proxyThread, URL, proxyheaders, proxies, aio, cpath, interval, cookies
        )
        self.asyncsize = asyncsize
        self.headers = {}
        self.maxtry = maxtry
        self.headerlist = headers
        self.timeout = timeout
        self.cpath = cpath
        self.session = {i: requests.Session() for i in range(0, asyncsize)}
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.asyncsize
        ) as excuter:
            futures = excuter.map(
                self.init_headers,
                [i for i in range(0, asyncsize)],
                [True for i in range(0, asyncsize)],
            )
            count = 0
            for f in futures:
                self.headers[count] = f
                count += 1

    def init_headers(self, sid=0, init=False):
        """
        Initialize headers and proxy for a session.

        Args:
            sid (int): Session ID.
            init (bool): Flag indicating whether it's an initial setup.

        Returns:
            The updated headers.
        """
        self.session[sid].close()
        self.session[sid] = requests.Session()
        self.headers[sid] = self.headerlist
        try:
            self.proxy[sid] = (init and self.proxy.get(sid)) or self.getRandomProxy()
        except:
            self.init_headers(sid)
        pass

    def __del__(self):
        """
        Destructor method.
        """
        return super().__del__()

    def fetch(self, url, method="get", sid=0, retry=0, **kwargs):
        """
        Perform an HTTP request to the specified URL.

        Args:
            url (str): URL to fetch.
            method (str): HTTP method to use (get or post).
            sid (int): Session ID.
            retry (int): Number of retries (used for recursion).
            **kwargs: Additional keyword arguments to be passed to the requests library.

        Returns:
            The response object, or None if the request fails.
        """
        kwargs["headers"] = self.headers[sid]
        kwargs["proxies"] = self.proxy[sid]
        try:
            if method == "post":
                r = self.session[sid].post(url, timeout=self.timeout, **kwargs)
            else:
                r = self.session[sid].get(url, timeout=self.timeout, **kwargs)
            print(f"{r.status_code} : {url}", kwargs.get("params"))
        except Exception as e:
            traceback.print_exc(file=self.logfile)
            print(e)
            time.sleep(1)
            self.init_headers(sid=sid)
            if retry < 10:
                if not self.maxtry:
                    retry += 1
                return self.fetch(url, method=method, sid=sid, retry=retry, **kwargs)
            else:
                return None
        if r.status_code not in [200, 404, 400]:
            print(url)
            print(method)
            print(kwargs)
            self.init_headers(sid=sid)
            if retry < 10:
                if not self.maxtry:
                    retry += 1
                retry += 1
                return self.fetch(url, method=method, sid=sid, retry=retry, **kwargs)
            else:
                return None
        return r


class okHTTpClient(ProxyServer):
    """
    Initialize the okHTTpClient class.

    Args:
        proxyThread (bool): Flag indicating whether to run the proxy update thread.
        URL (str): URL for the server.
        headers (dict): Headers to be used in the HTTP requests.
        proxyheaders (dict): Headers specific to the proxy server.
        proxies (dict): Proxies to use.
        aio (bool): Flag indicating whether to use AioScraper or AsyncScraper.
        cpath (str): Path to store files.
        asyncsize (int): Number of asynchronous sessions to use.
        interval (int): Interval for updating the proxy list (in seconds).
        cookies (bool): Flag indicating whether to handle cookies or not.
    """

    def __init__(
        self,
        proxyThread=True,
        URL="https://www.google.com",
        headers={},
        proxyheaders={},
        proxies={},
        aio=True,
        cpath="",
        asyncsize=1,
        interval=300,
        cookies=False,
    ) -> None:
        super().__init__(
            proxyThread, URL, proxyheaders, proxies, aio, cpath, interval, cookies
        )
        self.headers = headers or {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
        }
        self.Async = False
        self.asyncsize = asyncsize
        self.session = {i: requests.Session() for i in range(0, asyncsize)}
        self.headers = [self.headers for i in range(0, asyncsize)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=asyncsize) as excuter:
            futures = excuter.map(
                self.init_headers,
                [i for i in range(0, asyncsize)],
                [True for i in range(0, asyncsize)],
            )
            count = 0
            for f in futures:
                pass
        self.timeout = 5
        self.cpath = cpath

    def init_headers(self, sid=0, init=False):
        """
        Initialize headers and proxy for a session.

        Args:
            sid (int): Session ID.
            init (bool): Flag indicating whether it's an initial setup.
        """
        self.session[sid].close()
        self.session[sid] = requests.Session()
        try:
            self.proxy[sid] = (init and self.proxy.get(sid)) or self.getRandomProxy()
        except:
            self.getProxyList()
        pass

    def fetch(self, url, method="get", sid=0, retry=0, **kwargs):
        """
        Perform an HTTP request to the specified URL.

        Args:
            url (str): URL to fetch.
            method (str): HTTP method to use (get or post).
            sid (int): Session ID.
            retry (int): Number of retries (used for recursion).
            **kwargs: Additional keyword arguments to be passed to the requests library.

        Returns:
            The response object, or None if the request fails.
        """
        kwargs["headers"] = self.headers[sid]
        kwargs["proxies"] = self.proxy[sid]

        try:
            if method == "post":
                r = self.session[sid].post(url, timeout=self.timeout, **kwargs)
            else:
                r = self.session[sid].get(url, timeout=self.timeout, **kwargs)
            print(
                f"{r.status_code} : {kwargs['proxies']} {sid} {kwargs['headers'].get('Cookie')}"
            )
        except Exception as e:
            traceback.print_exc(file=self.logfile)
            print(e)
            time.sleep(1)
            self.init_headers(sid=sid)
            if retry < 10:
                retry += 1
                return self.fetch(url, method=method, sid=sid, retry=retry, **kwargs)
            else:
                return None
        if r.status_code not in [200, 404, 400]:
            print(r.status_code)
            print(url)
            print(method)
            print(kwargs)
            self.init_headers(sid=sid)
            if retry < 10:
                retry += 1
                return self.fetch(url, method=method, sid=sid, retry=retry, **kwargs)
            else:
                return None
        return r.json()

    def get(self, url, sid=0, params={}):
        """
        Perform a GET request to the specified URL.

        Args:
            url (str): URL to fetch.
            sid (int): Session ID.
            params (dict): Query parameters.

        Returns:
            The response object.
        """
        jsonbody = {
            "url": f"{url}?{urlencode(params)}",
            "headers": self.headers[sid],
            "proxy": self.proxy[sid]["http"],
        }
        r = self.session[sid].post("http://127.0.0.1:8001/makerequest", json=jsonbody)
        j = r.json()
        return r

    def post(self, url, sid=0, params={}, body={}):
        """
        Perform a POST request to the specified URL.

        Args:
            url (str): URL to fetch.
            sid (int): Session ID.
            params (dict): Query parameters.
            body (dict): Request body.

        Returns:
            The response object.
        """
        jsonbody = {
            "url": f"{url}?{urlencode(params)}",
            "headers": self.headers[sid],
            "proxy": self.proxy[sid]["http"],
            "body": body,
            "method": "post",
        }
        print(jsonbody)
        r = self.session[sid].post("http://127.0.0.1:8001/makerequest", json=jsonbody)
        return r

    def initAsycSession(self):
        self.AsyncSession = AsyncHTMLSession()

    def killAsycSession(self):
        self.AsyncSession.close()

    async def Asyncget(self, url, sid=0, params={}):
        """
        Perform an asynchronous GET request to the specified URL.

        Args:
            url (str): URL to fetch.
            sid (int): Session ID.
            params (dict): Query parameters.

        Returns:
            The response object.
        """
        try:
            jsonbody = {
                "url": f"{url}?{urlencode(params)}",
                "headers": self.headers,
                "proxy": self.proxy[sid]["http"],
            }
            r = await self.AsyncSession.post(
                "http://127.0.0.1:8001/makerequest", json=jsonbody
            )
            r = await r.json()
            return r
        except:
            self.init_headers(sid)
            return await self.Asyncget(url, sid)

    def __del__(self):
        return super().__del__()


def splitListInNpairs(li, interval):
    """
    Split a list into pairs of a given interval.

    Args:
        li (list): The list to split.
        interval (int): The interval to split the list.

    Returns:
        List: A list containing pairs of the given interval.
    """
    ran = len(li) / interval
    ran = int(ran) if ran == int(ran) else int(ran) + 1
    flist = []
    for i in range(0, ran):
        item = li[interval * i : interval * (i + 1)]
        flist.append(item)
    return flist


def GetAdInfo(ads, sid, ob):
    """
    Get information for an ad.

    Args:
        ads (dict): The ad information.
        sid (int): Session ID.
        ob (okHTTpClient): The HTTP client object.

    Returns:
        str: A string indicating the result.
    """
    adid = ads["id"]
    url = f"https://api.pap.fr/app/annonces/detail"
    dic = {"id": adid}
    url = f"{url}?{urlencode(dic)}"
    r = ob.get(url, sid)
    ad = r.get("annonce")
    with open(f"outputfilenamethiqs.json", "a") as file:
        file.write(json.dumps(ad) + "\n")
    return "dome"


if __name__ == "__main__":
    cpath = os.path.dirname(__file__) or "."
    headers = {
        "Accept": "*/*",
        "X-App-Version": "4.0.10",
        "X-App-Target": "android",
        "X-App-Uuid": "b8c75b38-b638-4269-acaf-722f42936bec",
        "User-Agent": "PAP/G-4.0.10 (Google sdk_gphone_x86 Android SDK 30) okhttp/5.0.0-alpha.2",
        "Host": "api.pap.fr",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
    }
    asyncsize = 1
    ob = okHTTpClient(
        True,
        URL="https://www.pap.fr",
        aio=False,
        cpath=cpath,
        headers=headers,
        asyncsize=asyncsize,
    )
    dic = {
        "type": "recherche",
        "produit": "vente",
        "geo[ids][]": "25",
    }
    url = "https://api.pap.fr/app/annonces"
    url = f"{url}?{urlencode(dic)}"
    r = ob.get(url)
    ads = r["annonces"]
    adlist = splitListInNpairs(ads, asyncsize)
    for ads in adlist:
        with concurrent.futures.ThreadPoolExecutor(max_workers=asyncsize) as excuter:
            adsidlist = ads
            print(adsidlist)
            futures = excuter.map(
                GetAdInfo,
                adsidlist,
                [i for i in range(0, len(adsidlist))],
                [ob for i in range(0, len(adsidlist))],
            )
            for f in futures:
                pass
    ob.__del__()
