from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
import time

from selenium.webdriver.support.ui import WebDriverWait

import undetected_chromedriver.v2 as uc
from pprint import pformat


def event_details(eventdata):
    # print(pformat(eventdata))
    pass


class Ad:
    def __init__(self):
        # initialize the browser window
        try:
            chrome_options = uc.ChromeOptions()  # new solution
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--start-maximized")
            # chrome_options.add_argument(f"--proxy-server={proxy}")
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36")
            # chrome_options.binary_location = "C:/Program Files/Google/Chrome/Application/chrome.exe"
            self.driver = uc.Chrome(ChromeDriverManager().install(), options=chrome_options, enable_cdp_events=True)
            self.driver.add_cdp_listener('Network.requestWillBeSent', event_details)
            self.driver.add_cdp_listener('Network.dataReceived', event_details)
        except Exception as e:
            print(e)

    def visit_url(self):
        url = 'https://www.leboncoin.fr/locations/2079577822.htm'
        self.driver.get(url)
        time.sleep(10)
        self.driver.quit()
