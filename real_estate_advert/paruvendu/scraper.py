try:

    import sys
    import pickle
    import os
    from webdriver_manager.chrome import ChromeDriverManager
    # from fp.fp import FreeProxy
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

    from kafka_publisher import KafkaTopicProducer

    print('all module are loaded ', os.getcwd())

except Exception as e:
    print("Error ->>>: {} ".format(e))

global driverinstance
driverinstance = None
producer = KafkaTopicProducer()

class Spoofer(object):

    def __init__(self, country_id=['FR'], rand=True, anonym=True):
        self.country_id = country_id
        self.rand = rand
        self.anonym = anonym
        self.userAgent, self.ip = self.get()

    def get(self):
        ua = UserAgent()
        ip = f"170.155.5.235:8080"
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
        self.options.add_argument("--headless")
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
        # webdriver.DesiredCapabilities.CHROME['acceptSslCerts'] = True

        path = os.path.join(os.getcwd(), 'chromedriver')

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

def scrape_ad(url, driver):
    time.sleep(2)
    global driverinstance
    driver = driverinstance
    # producer = KafkaTopicProducer()

    driver.get(url)
    list_ads = driver.find_elements(by=By.XPATH,
                                    value="//div[@class=' ergov3-annonce ' or @class='lazyload_bloc ergov3-annonce ' ]")

    for i in range(1, len(list_ads)):
        url = list_ads[i].find_elements(By.TAG_NAME, 'a')[-1].get_attribute('href')
        scrape_ad_url(url, driver)


def extract_html(URL):
    # Open link on new window -> get the source code -> close the code
    driver.execute_script("window.open('');")
    WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
    driver.switch_to.window(driver.window_handles[1])
    driver.get(URL)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.execute_script("window.close();")
    driver.switch_to.window(driver.window_handles[0])
    return soup


# def close_notification_pop_up():
#     global driverinstance
#     driver = driverinstance

#     try:
#         driver.find_element('')

def scrape_ad_url(url, driver):
    print('here')
    ad_data = {"image_urls": [], "3d_view": "", "location": "", "title": "", "price": "",
               "description": "", "location_url": "", "opening_timings": "", "features": [],
               "visting_form": "", "connectivity_index": "", "fiber_eligibility_rate": "",
               "vendor_information": {"vendor_type": "", "vendor_url": "", "vendor_name": "",
                                      "advertisement": "", "logo_url": "", "fb_url": "",
                                      "vendor_location": "", "opening_time": ""}}

    ## open new page
    driver.execute_script("window.open('');")
    WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
    driver.switch_to.window(driver.window_handles[1])
    driver.get(url)

    try:
        ad_data['title'] = driver.find_element(By.XPATH, '//*[@id="detail_h1"]').text.strip()
    except:
        print('ERROR: -------> title ')

    try:
        # ad_data['location'] = driver.find_element(By.XPATH, '//*[@id="detail_loc"]').text.strip()
        ad_data['location'] = driver.find_element(By.CLASS_NAME, "fcbx_localisation2").text.strip()
        ad_data['location_url'] = driver.find_element(By.CLASS_NAME, "fcbx_localisation2").get_attribute('url')
    except:
        print('ERROR: -------> location ')

    try:
        ad_data['description'] = driver.find_element(By.XPATH,
                                                     '//div[@class="im12_txt_ann im12_txt_ann_auto"]').text.strip()
    except:
        print('ERROR: -------> Description ')

    try:
        uls = driver.find_elements(By.XPATH, '//ul[@class="crit-alignbloc"]')
        for i in uls:
            li = BeautifulSoup(i.get_attribute('innerHTML'), 'html.parser')
            for j in li.find_all('li'):
                ad_data['features'].append(j.text.strip())
    except:
        print('ERROR: -------> Features ')

    try:
        ad_data["opening_timings"] = driver.find_element(By.XPATH,
                                                         '//div[@class="opt19_listlinks opt19_listhor"]').text.strip()
    except:
        print('ERROR: -------> Timing ')

    try:
        ad_data['price'] = driver.find_element(By.XPATH, '//div[@id="autoprix"]').text.strip()
    except:
        print('ERROR: -------> Price ')

    try:
        ad_data['visting_form'] = driver.find_element(By.CLASS_NAME, 'im12_photosvoir').get_attribute('href')
    except:
        print('ERROR: -------> Visiting Form ')

    try:
        ad_data['vendor_information']['fb_url'] = driver.find_elements(By.XPATH, "//div[@class='opt19_listlinks']/a")[
            2].get_attribute('href')
    except:
        print('ERROR: -------> Vendor fb url ')

    try:
        vendor = driver.find_element(By.CLASS_NAME, 'infovendeur')
        try:
            ad_data['vendor_information']['vendor_url'] = vendor.find_element(By.XPATH,
                                                                              '//div[@id="detail_infosvendeur"]/a').get_attribute(
                'href')
        except:
            print('ERROR: -------> Vendor URL ')

        try:
            ad_data['vendor_information']['vendor_name'] = vendor.find_element(By.ID,
                                                                               'detail_infosvendeur').text.strip()
        except:
            print('ERROR: -------> Vendor Name ')

        try:
            ad_data['vendor_information']['logo_url'] = vendor.find_element(By.XPATH,
                                                                            '//div[@id="detail_infosvendeur"]//img').get_attribute(
                'src')
        except:
            print('ERROR: -------> Vendor logo URL ')

        try:
            ad_data['vendor_information']['vendor_location'] = driver.find_element(By.XPATH,
                                                                                   "(//div[@class='opt19_listlinks'])[4]").text.strip()
        except:
            print('ERROR: -------> Vendor location ')

        try:
            ad_data['vendor_information']['opening_time'] = driver.find_element(By.CLASS_NAME,
                                                                                'opt19_listhor').text.strip()
        except:
            print('ERROR: -------> Vendor OPENIng time ')

        try:
            temp = driver.find_element(By.XPATH, '//ul[@class="im12_listservices"]/li/a').get_attribute('href')
            ad_data['visiting_form'] = temp if temp != 'javascript:void()' else ""
        except:
            print('ERROR: -------> visiting_form ')

        try:
            conn = driver.find_element(By.CLASS_NAME, 'qualiteconnexion_zonetiers')
            ad_data['connectivity_index'] = conn.find_element(By.XPATH,
                                                              '//div[@class="indice_signif"]/span').text.strip()
        except:
            print('ERROR: -------> connectivity index ')

        try:
            conn = driver.find_element(By.CLASS_NAME, 'qualiteconnexion_zonetiers')
            ad_data['connectivity_index'] = conn.find_element(By.XPATH,
                                                              '//div[@class="indice_signif"]/span').text.strip()
        except:
            print('ERROR: -------> connectivity index ')

        try:
            ad_data['fiber_eligibility_rate'] = driver.find_element(By.XPATH,
                                                                    '//div[@class="qualiteconnexion_zonetiers"]//div[@class="indice_signif"]/span').text.strip()
        except:
            print('ERROR: -------> Fiber Eligibility Rate ')


    except:
        print('ERROR: -------> Vendor Information  ')

    ## open photo gallery
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "(//a[@title='Photo annonce']//span)[3]")))
        driver.find_element(By.XPATH, "(//a[@title='Photo annonce']//span)[3]").click()
        # WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "(//a[@title='Photo annonce']//span)[3]")))
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "flex-viewport")))
        srcs = BeautifulSoup(driver.find_element(By.CLASS_NAME, 'flex-viewport').get_attribute('innerHTML'),
                             'html.parser')
        for i in srcs.find_all('img'):
            ad_data['image_urls'].append(i['src'])
    except Exception as e:
        try:
            ad_data['image_urls'].append(driver.find_element(By.CSS_SELECTOR, 'img#pic_main').get_attribute('src'))
        # except:
        except Exception as e:
            print('ERROR -------> not able to add main image')
        print('ERROR: ------->  Images ')

    ## 3d View
    try:
        ad_data['3d_view'] = driver.find_element(By.XPATH, '//li[@class="clone"]/a').get_attribute('href')
    except:
        print('ERROR: -------> 3d_view ')

    print(f"\n\n\n {url} -----------------------------------\n")
    producer.kafka_producer_sync(topic="paruvendu-data", data=ad_data)
    ## close the tab
    driver.execute_script("window.close();")
    driver.switch_to.window(driver.window_handles[0])


def scrape_pages(page_url, driver, total=10):
    print("======================>")
    for p in range(0, total):
        if p != 0:
            page_url = page_url + f"&p={p}"
            # scrape_ad(url=page_url, page_number=p)
            scrape_ad(page_url, driver)


def scrape_rental_ads(mini_price, maxi_price, text, driver):
    """
     tt=1 sale house : Vente Maison
     tt=2 sale land : Vente terrain
     tt=5 location : rental

     ddlAffine = "text"
     px0  minimum price
     px1 maximum price
     codeINSEE
    """
    rental_code = "5"
    total_pages = 5
    min_price = f"&px0={mini_price}"
    max_price = f"&px0={maxi_price}"
    codeINSEE = "&codeINSEE= "

    if len(text) > 3:
        text = f"&ddlAffine={text}"
        url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1{min_price}{max_price}&pa=FR&lol=0&ray=50{text} {codeINSEE},"
        scrape_pages(url, total=total_pages, driver=driver)


def scrape_sale_ads(mini_price, maxi_price, text):
    rental_code = "1"
    total_pages = 5
    min_price = f"&px0={mini_price}"
    max_price = f"&px0={maxi_price}"
    codeINSEE = "&codeINSEE= "
    if len(text) > 3 and mini_price > 0 and maxi_price > 0:
        text = f"&ddlAffine={text}"
        url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1{min_price}{max_price}&pa=FR&lol=0&ray=50{text} {codeINSEE},"
    else:
        url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&pa=FR&lol=0&ray=50{codeINSEE},"

    scrape_pages(url, total=total_pages)
    driverinstance.quit()


def main_scraper(paylaod):
    paylaod = {'text': 'house', 'min_price': 0.0, 'max_price': 0.0, 'city': 'string', 'rooms': 0,
               'real_state_type': 'rental'}
    driver = WebDriver()
    global driverinstance
    driverinstance = driver.driver_instance
    url = "https://www.paruvendu.fr"

    accept_cookies(driverinstance, url)

    if paylaod["real_state_type"] == "sale":
        scrape_sale_ads(paylaod["min_price"], paylaod["max_price"], paylaod["text"])

    elif paylaod["real_state_type"] == "rental":
        scrape_rental_ads(paylaod["min_price"], paylaod["max_price"], paylaod["text"], driverinstance)