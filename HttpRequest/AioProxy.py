import json
import asyncio, aiohttp
import time
from aiosocksy.connector import ProxyConnector, ProxyClientRequest

restext = []


class ProxyScraper:

    """
    Class for scraping and checking proxy servers.
    """

    def __init__(self, url=None, headers=None, cookies=False) -> None:
        """
        Initialize the ProxyScraper class.

        Args:
            url (str): URL for checking the proxies.
            headers (dict): Headers to be used in the HTTP requests.
            cookies (bool): Flag indicating whether to handle cookies or not.
        """

        self.protocols = ["http", "socks4", "socks5"]
        self.urls = []
        self.cookies = cookies
        self.url = url
        self.headers = headers
        self.proxylist = []
        for protocol in self.protocols:
            self.urls.append(
                [
                    f"https://api.proxyscrape.com/v2/?request=getproxies&protocol={protocol}&timeout=10000&country=all&ssl=all&anonymity=all",
                    protocol,
                ]
            )
        pass

    async def fetch(self, url, **kwargs):
        """
        Perform an HTTP GET request to the specified URL.

        Args:
            url (str): URL to fetch.
            **kwargs: Additional keyword arguments to be passed to aiohttp's session.get().

        Returns:
            The response content or status code, depending on the keyword arguments passed.
        """
        connector = ProxyConnector()
        async with aiohttp.ClientSession(
            connector=connector, request_class=ProxyClientRequest
        ) as session:
            print(url)
            res = await session.get(url, ssl=False, **kwargs)
            d = res.status
            print(d)
            if kwargs.get("proxy"):
                if self.cookies and d == 200:
                    cookies = res.cookies.output(header="", sep=";").strip()
                    if cookies and ("__cf_bm=" in cookies or "datadome" in cookies):
                        d = d, cookies
                    else:
                        d = 403, ""
                pass
            else:
                d = await res.read()
            return d

    async def main(self):
        """
        Main method for scraping and checking proxies.

        Returns:
            The list of working proxies.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        }
        tasks = [
            asyncio.ensure_future(self.getProxy(url, protocol, headers))
            for url, protocol in self.urls
        ]

        await asyncio.gather(*tasks)
        self.proxylist = []
        await asyncio.gather(asyncio.ensure_future(self.checkproxy()))
        return self.proxylist

    async def getProxy(self, url, protocol, headers):
        """
        Fetch and save proxies for a specific protocol.

        Args:
            url (str): URL to fetch proxies from.
            protocol (str): Protocol of the proxies.
            headers (dict): Headers to be used in the HTTP requests.
        """
        global restext
        content = await self.fetch(url, headers=headers)
        with open(f"{protocol}.txt", "wb") as file:
            file.write(content)
        data = content.decode("utf-8")
        data = data.split("\r\n")
        return True

    def FetchNGetProxy(self):
        self.proxylist = asyncio.run(self.main())
        return self.proxylist

    def GetProxyList(self):
        return self.proxylist

    def save(self, cpath=None):
        """
        Save the list of working proxies to a file.

        Args:
            cpath (str): Path where the file should be saved.
        """

        final = ""
        for proxy in self.proxylist:
            final += json.dumps(proxy) + "\n"
        with open(f"{cpath}/working.txt", "w") as file:
            file.write(final)

    async def checkproxy(self):
        """
        Check the working status of the fetched proxies.
        """
        for protocol in self.protocols:
            with open(f"{protocol}.txt", "r") as file:
                proxies = file.readlines()
                tasks = []
                data = []
                for proxy in proxies:
                    proxy = proxy.replace("\n", "")
                    tasks.append(
                        asyncio.ensure_future(
                            self.check_if_proxy_is_working(proxy, protocol, self.url)
                        )
                    )
                    if len(tasks) >= 100:
                        data += await asyncio.gather(*tasks)
                        tasks = []
                data += await asyncio.gather(*tasks)
                working = ""
                for d in data:
                    if d:
                        r, proxy = d
                        if r == 200:
                            working += json.dumps(proxy) + "\n"
                            self.proxylist.append(proxy)
                print(working)
                lent = {len(data)}
                print(f"{protocol} done {lent} proxies")

    async def check_if_proxy_is_working(self, proxies, protocol, url):
        """
        Check if a proxy is working

        Args:
        proxies (str): Proxy server address.
        protocol (str): Protocol of the proxy.
        url (str): URL to check the proxy against.

        Returns:
            A tuple containing the response status code and the proxy if it is working, or False otherwise.
        """

        proxies = {
            "http": f"{protocol}://{proxies}",
            "https": f"{protocol}://{proxies}",
        }
        try:
            print(proxies)
            if self.cookies:
                r, cookies = await self.fetch(
                    url, proxy=proxies["http"], headers=self.headers, timeout=10
                )
                proxies["cookies"] = cookies
            else:
                r = await self.fetch(
                    url, proxy=proxies["http"], headers=self.headers, timeout=10
                )
            print(r)
            return r, proxies
        except:
            pass
        return False


def getProxyasstring():
    # Get the current time for measuring execution time
    stat = time.time()
    asyncio.run(main())
    return restext


if __name__ == "__main__":
    stat = time.time()
    LEBONCOIN_CHECK_URL = "https://api.leboncoin.fr/finder/classified/2130999715"
    headers = {
        "Accept-Language": "en-US,en;q=0.8,fr;q=0.6",
        "Accept-Encoding": "*",
        "User-Agent": "LBC;Android;11;sdk_gphone_x86;phone;8b1263fac1529be6;wifi;5.70.2;570200;0",
    }
    ob = ProxyScraper(LEBONCOIN_CHECK_URL, headers)
    ob.FetchNGetProxy()
    ob.save()
