try:
    import sys
    import pickle
    import os
    # from webdriver_manager.chrome import ChromeDriverManager
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
    import undetected_chromedriver as uc
    import time
    from kafka_publisher import KafkaTopicProducer
    from .capachasolver_v2 import isCaptcha
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
        ip = f"169.57.1.85:8123"
        return ua.random, ip


class DriverOptions(object):

    def __init__(self):
        # self.options = Options()
        self.options = uc.ChromeOptions()
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument('--disable-popup-blocking')
        # self.options.add_argument('--no-sandbox')
        # self.options.add_argument('--start-maximized')
        # # self.options.add_argument('--start-fullscreen')
        # self.options.add_argument('--single-process')
        # self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument("--incognito")
        self.options.add_argument("--headless")
        # self.options.add_argument('--disable-blink-features=AutomationControlled')
        # self.options.add_argument('--disable-blink-features=AutomationControlled')
        # self.options.add_experimental_option('useAutomationExtension', False)
        # self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # self.options.add_argument("disable-infobars")

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

        # driver = webdriver.Chrom(ChromeDriverManager().install(), options=self.options)
        # driver = webdriver.Chrome(options=self.options)
        driver = uc.Chrome(options=self.options)

        return driver

def getStatus():
    with open("status.txt",'r') as file:
        return file.read()

    
def accept_cookies(driver, url):
    try:
        # click on accept cookies popup
        driver.get(url)
        time.sleep(4)
        while True:
            print('solving ...')
            wait = WebDriverWait(driver, 5)
            isCaptcha(driver)
            break

        wait = WebDriverWait(driver, 10)
        wait.until(EC.element_to_be_clickable((By.ID, 'didomi-notice-agree-button')))
        button =driver.find_element(By.ID, 'didomi-notice-agree-button') 
        if button:
            button.click()
        driver.get_cookies()
        return True
    except Exception as e:
        print(e)
    return False

def scrape_ad_url(url, driver):
    ad_data = { "image_urls":[], "3d_view": "", "location": "", "title": "", "price": "",
            "rooms" : "", "area" : "", "date" : "",
               "description": "", 
               "vendor_information": {"vendor_type": "", "vendor_url": "", "vendor_name": "",
                                      "advertisement": "", "logo_url": "", "fb_url": "",
                                       "vendor_location" :"", "opening_time": ""}}
               
    ## open new page
    try:
        driver.execute_script("window.open('');")
        WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
        driver.switch_to.window(driver.window_handles[1])
        driver.get(url)
        solved = False
        while not solved:
            solved = isCaptcha(driver)
            if solved:
                driver.get(url)
            else: 
                break
        
        try:
            driver.find_element(By.XPATH,'//button[@class="styles_ImageFit__eCNtA styles_cover__Zzp_L styles_fullWidth__r3KVX styles_clickable__YVsVw _3s2p-"]').click()
            divs = driver.find_elements(By.CLASS_NAME, "slick-slide")
            for i in divs:
                img= i.find_element(By.TAG_NAME, 'img').get_attribute('src')
                if img not in ad_data['image_urls']:
                    ad_data['image_urls'].append(img)
            driver.find_element(By.XPATH, '//button[@title="Fermer"]').click()
        except:
            print("ERR ----- image")
            
        try:
            ad_data['title'] = driver.find_element(By.XPATH, '//h1[@data-qa-id="adview_title"]').text
        except:
            print("ERR ------ title")
        try:
            ad_data['rooms'] = driver.find_element(By.XPATH, '//div[@data-qa-id="adview_spotlight_description_container"]/div[2]/div/span').text.strip()
        except:
            print("ERR ------ rooms")
        try:
            ad_data['area'] = driver.find_element(By.XPATH, '//div[@data-qa-id="adview_spotlight_description_container"]/div[2]/div/span[2]').text.strip()
        except:
            print("ERR ------ area")
        try:
            ad_data['date'] = driver.find_element(By.XPATH, '//div[@data-qa-id="adview_spotlight_description_container"]/div[3]').text.strip()
        except:
            print("ERR ------ date")
        try:
            ad_data['price'] = driver.find_element(By.XPATH, '//div[@data-qa-id="adview_spotlight_description_container"]/div[2]/div[3]').text.strip()
        except:
            print("ERR ------ price")
        try:
            try:
                driver.find_element(By.XPATH, '//button[@class="_27ngl Roh2X _2NG-q _29R_v HGqCc _3Q3XS Mb3fh _137P- _35DXM P4PEa wEezs"]').click()
            except:
                print('')
            ad_data['description'] = driver.find_element(By.XPATH, '//div[@data-qa-id="adview_description_container"]').text.strip()
        except:
            print("ERR ------ description")
        try:
            ad_data['vendor_information']['vendor_name'] = driver.find_elements(By.XPATH, '//div[@data-qa-id="adview_contact_container"]//a')[1].text.strip()
        except:
            print('vendor name')
        print(ad_data)
        producer.kafka_producer_sync(topic="leboncoin-data", data=ad_data)
        
        driver.execute_script("window.close();")
        driver.switch_to.window(driver.window_handles[0])
    except:
        print('asdfalksdfj;lasjdkf;lkjasdl;fkj')

        



def scrape_ads( driver, url):
    # driver.get(url)
    while True:
        a = driver.find_elements(By.CLASS_NAME,'styles_adCard__HQRFN')
        print(f'{len(a)} urls found')
        for i in a:
            scrape_ad_url(i.find_element(By.TAG_NAME, 'a').get_attribute('href'), driver)
        try:
            next_page = driver.find_element(By.XPATH, '//a[@title="Page suivante"]').get_attribute('href')
            driver.get(next_page)
            driver.implicitly_wait(5)
            solved = False
            while not solved:
                solved = isCaptcha(driver)
                if solved:
                    driver.get(url)
                else: 
                    break

        except Exception as e:
            print(e)
            break


        
def main_scraper():
    paylaod = {'text': 'house', 'min_price': 0.0, 'max_price': 0.0, 'city': 'string', 'rooms': 0,
               'real_state_type': 'sale'}
    driver = WebDriver()
    global driverinstance
    driverinstance = driver.driver_instance
    url = "https://www.leboncoin.fr/"
    solved = False
    while not solved:
        response = driverinstance.get('https://www.leboncoin.fr/_immobilier_/offres')
        if response != None:
            solved  = accept_cookies(driverinstance, url)
        else:
             break

    time.sleep(5)
    # driverinstance.find_element(By.XPATH, "//button[contains(@class,'_2qvLx _3WXWV')]").click()
    # driverinstance.implicitly_wait(5)

    print(driverinstance.current_url)
    scrape_ads(driverinstance, 'https://www.leboncoin.fr/_immobilier_/offres')

