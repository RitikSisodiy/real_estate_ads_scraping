try:

    import sys
    import pickle
    import os
    from webdriver_manager.chrome import ChromeDriverManager
    from fp.fp import FreeProxy
    from fake_useragent import UserAgent
    from bs4 import BeautifulSoup
    from selenium import webdriver
    import random
    from selenium.webdriver import Chrome
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    import time

    print('all module are loaded ')

except Exception as e:
    print("Error ->>>: {} ".format(e))


class Spoofer(object):

    def __init__(self, country_id=['FR'], rand=True, anonym=True):
        self.country_id = country_id
        self.rand = rand
        self.anonym = anonym
        self.userAgent, self.ip = self.get()

    def get(self):
        ua = UserAgent()
        ip = f"145.239.226.146:1080"
        return ua.random, ip


class DriverOptions(object):

    def __init__(self):
        self.options = Options()
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--start-maximized')
        # self.options.add_argument('--start-fullscreen')
        self.options.add_argument('--single-process')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument("--incognito")
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_argument("disable-infobars")

        self.helperSpoofer = Spoofer()

        self.options.add_argument('user-agent={}'.format(self.helperSpoofer.userAgent))
        # self.options.add_argument('--proxy-server=%s' % self.helperSpoofer.ip)


class WebDriver(DriverOptions):

    def __init__(self, path=''):
        DriverOptions.__init__(self)
        self.driver_instance = self.get_driver()

    def get_driver(self):
        print("""
        IP:{}
        UserAgent: {}
        """.format(self.helperSpoofer.ip, self.helperSpoofer.userAgent))

        # PROXY = self.helperSpoofer.ip
        # webdriver.DesiredCapabilities.CHROME['proxy'] = {
        #     "httpProxy": PROXY,
        #     "ftpProxy": PROXY,
        #     "sslProxy": PROXY,
        #     "noProxy": None,
        #     "proxyType": "MANUAL",
        #     "autodetect": False
        # }
        webdriver.DesiredCapabilities.CHROME['acceptSslCerts'] = True

        path = os.path.join(os.getcwd(), 'chromedriver.exe')

        driver = webdriver.Chrome(ChromeDriverManager().install(), options=self.options)

        return driver


def accept_cookies(driver, url):
    try:
        # click on accept cookies popup
        driver.get(url)
        wait = WebDriverWait(driver, 5)
        wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Accepter et Fermer']")))
        button = driver.find_element_by_xpath("//button[text()='Accepter et Fermer']")
        if button:
            button.click()
        driver.get_cookies()
        return True
    except Exception as e:
        print(e)
    return False


def strip_text(text):
    result = text.replace('\n', '')
    result = result.replace('\t', '')
    return result


def scrapelist(driver, url):
    time.sleep(2)
    page_data = []
    driver.get(url)

    list_ads = driver.find_elements(by=By.XPATH,
                                    value="//div[@class=' ergov3-annonce ' or @class='lazyload_bloc ergov3-annonce ' ]")
    page_down = 0
    for index, _ad in enumerate(list_ads):
        # image url , number of images , property name , city , price , text ,
        ad_data = {"image_url": "", "city": "", "title": "", "price": "",
                   "description": "", "url": "", "number_of_photos": "",
                   "vendor_information": {"vendor_type": "", "vendor_url": "", "vendor_name": "",
                                          "advertisement": "", "logo_url": ""}}
        try:
            ad_data["image_url"] = _ad.find_element(by=By.XPATH, value=".//img[@class='imgblur']").get_attribute('src')
            ad_data["url"] = _ad.find_element(by=By.XPATH, value=".//*[@class='voirann']").get_attribute('href')
            soup = BeautifulSoup(_ad.find_element(by=By.TAG_NAME, value="h3").get_attribute('innerHTML'), "html.parser")
            ad_data["title"] = strip_text(soup.text)
            ad_data["number_of_photos"] = _ad.find_element(by=By.XPATH, value=".//span[@class='re14_nbphotos']").text
            ad_data["city"] = strip_text(soup.find("cite").text)

            ad_data["price"] = _ad.find_element(by=By.XPATH, value=".//div[@class='ergov3-priceannonce']").text
            ad_data["description"] = strip_text(_ad.find_element(by=By.XPATH, value=".//p[@class='txt-long']").text)

            soup = BeautifulSoup(
                _ad.find_element(by=By.XPATH, value=".//*[@class='enslogoname_2lines']").get_attribute("innerHTML"))

            ad_data["vendor_information"]["logo_url"] = soup.find("img").get('src')
            try:
                ad_data["vendor_information"]["vendor_type"] = strip_text(soup.findAll("span")[0].text)
                ad_data["vendor_information"]["vendor_name"] = strip_text(soup.findAll("span")[1].text)
                ad_data["vendor_information"]["advertisement"] = strip_text(soup.find("a").get_text())
                ad_data["vendor_information"]["vendor_url"] = strip_text(soup.find("a").get("href"))
            except:
                pass

        except NoSuchElementException as e:
            print(e)

        if index % 3 == 0:
            time.sleep(random.randint(2, 4))
            body = driver.find_element_by_css_selector('body')
            body.send_keys(Keys.PAGE_DOWN)
            page_data.append(ad_data)

    return page_data


def scrape_pages(url, driverinstance, total=10, ):
    all_data = []
    for p in range(0, total):
        if p != 0:
            url = url + f"&p={p}"

        data = {"page_data": scrapelist(driverinstance, url=url), "page_number": p}
        all_data.append(data)

    return all_data


"""
 tt=1 sale house : Vente Maison
 tt=2 sale land : Vente terrain
 tt=5 location : rental 

 ddlAffine = "text"
 px0  minimum price 
 px1 maximum price
 codeINSEE

"""


def rental_ads(total_pages, dirver, mini_price, maxi_price, text):
    rental_code = "5"
    min_price = f"&px0={mini_price}"
    max_price = f"&px0={maxi_price}"
    codeINSEE = "&codeINSEE= "
    text = f"&ddlAffine={text}"

    url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1{min_price}{max_price}&pa=FR&lol=0&ray=50{text} {codeINSEE},"
    scrape_pages(url, driverinstance=dirver, total=total_pages)


def purchase_ads(total_pages, dirver, mini_price, maxi_price, text):
    rental_code = "1"
    total_pages = 5
    min_price = f"&px0={mini_price}"
    max_price = f"&px0={maxi_price}"
    codeINSEE = "&codeINSEE= "
    text = f"&ddlAffine={text}"
    url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1{min_price}{max_price}&pa=FR&lol=0&ray=50{text} {codeINSEE},"
    scrape_pages(url, driverinstance=dirver, total=total_pages)


def main():
    driver = WebDriver()
    driverinstance = driver.driver_instance
    url = "https://www.paruvendu.fr"
    name = "paruvendu"

    url = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt=5&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&lol=0&ray=50&codeINSEE=75000,"

    if accept_cookies(driverinstance, url):
        pass

    data = scrape_pages(driverinstance, total=5)
    with open("data.json", "w") as pdata:
        import json
        json.dump(data, pdata, indent=4)
    time.sleep(500)


if __name__ == "__main__":
    main()
