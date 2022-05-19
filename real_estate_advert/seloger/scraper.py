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
    import random
    from kafka_publisher import KafkaTopicProducer
    from capachasolver_v2 import isCaptcha
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

        path = os.path.join(os.getcwd(), '../../chromedriver')

        # driver = webdriver.Chrom(ChromeDriverManager().install(), options=self.options)
        # driver = webdriver.Chrome(options=self.options)
        driver = uc.Chrome(options=self.options)

        return driver


    
def accept_cookies(driver, url):
    try:
        # click on accept cookies popup
        driver.get(url)
        wait = WebDriverWait(driver, 5)
        time.sleep(5)
        # input('continue :')
        solved = False
        while not solved:
            solved = isCaptcha(driver, url)
            # if solved:
            #     driver.get(url)
            # else: 
            #     break

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
    # time.sleep(random.randint(10, 15))
    ad_data = { "image_urls":[], "3d_view": "", "location": "", "title": "", "price": "",
            "pieces" : "", "chambers" : "", "area" : "", "date" : "",
               "description": "", "features": [], "heat": "",
               "vendor_information": { "vendor_name": "",
                                       "number": "",
                                       "vendor_location" :"", "opening_time": ""}}
               
    ## open new page
    try:
        driver.execute_script("window.open('');")
        WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
        driver.switch_to.window(driver.window_handles[1])
        driver.get(url)
        driver.implicitly_wait(5)
        solved = False
        while not solved:
            solved = isCaptcha(driver, url)
            if solved:
                driver.get(url)
            else: 
                break
        driver.implicitly_wait(5)
        
        try:
            ad_data['title'] = driver.find_element(By.XPATH, '//div[contains(@class,"Summarystyled__Title-sc-1u9xobv-3")]').text.strip()
        except:
            print("ERR ------ title")
        try:
            ad_data['pieces'] = driver.find_element(By.XPATH, '//div[@class="Summarystyled__TagsWrapper-sc-1u9xobv-13 hRURQu"]/div[1]').text.strip()
        except:
            print("ERR ------ pieces")
        try:
            ad_data['chambers'] = driver.find_element(By.XPATH, '//div[@class="Summarystyled__TagsWrapper-sc-1u9xobv-13 hRURQu"]/div[2]').text.strip()
        except:
            print("ERR ------ chambers")
        try:
            ad_data['area'] = driver.find_element(By.XPATH, '//div[@class="Summarystyled__TagsWrapper-sc-1u9xobv-13 hRURQu"]/div[3]').text.strip()
        except:
            print("ERR ------ area")
        try:
            ad_data['price'] = driver.find_element(By.XPATH,"//span[contains(@class,'Summarystyled__PriceText-sc-1u9xobv-7')]").text.strip()
        except:
            print("ERR ------ price")
        try:
            try:
                driver.find_element(By.XPATH, '//div[contains(@class="ShowMoreText__UIShowMoreContainer-sc-1swit84-1")]').click()
            except:
                print('')
            ad_data['description'] = driver.find_element(By.XPATH,"//div[contains(@class,'ShowMoreText__UITextContainer-sc-1swit84-0')]").text.strip()
        except:
            print("ERR ------ description")
        try:
            ad_data['heat'] = driver.find_element(By.XPATH, "//div[@class='Diagnosticsstyled__TextWrapper-sc-kxy40a-9 hvCTlF']").text.strip()
        except:
            print("ERR ------ heat")
        try:
            try:
                driver.find_element(By.XPATH, '//button[@data-test="show-detail-feature-button-desktop"]').click()
            except:
                # no see more features
                print('')
            for i in range(1,15):
                ad_data['features'].append(driver.find_element(By.XPATH,f"//div[contains(@class,'generalFeatures')]/div/div[{i}]").text.strip().split('\n')[1:])
        except:
            print('features')
        try:
            driver.find_element(By.XPATH,"//button[contains(@class,'phone-button')]").click()
            ad_data['vendor_information']['number'] = driver.find_element(By.XPATH,"//div[contains(@class,'phone-button')]").text.strip()
        except:
            print('vendor number')
        try:
            ad_data['vendor_information']['vendor_name'] = driver.find_element(By.XPATH, '//h3[contains(@class,"LightSummarystyled__Title-sc-k5t1l5-1")]').text.strip()
        except:
            print('vendor name')
        try:
            driver.find_element(By.XPATH, '//div[@data-test="media-photo"]').click()
            total = int(driver.find_element(By.XPATH, '//div[@data-test="counter"]').text.split('/')[-1])
            print(total)
            for i in range(total  ):
                ad_data['image_urls'].append(driver.find_elements(By.XPATH, '//img[@data-test="high-res-image"]')[-2].get_attribute('src'))
                next_img = driver.find_element(By.XPATH, '//button[@data-testid="gsl.uilib.Slider.NextButton"]')
                time.sleep(1)
                driver.execute_script("arguments[0].click();", next_img)


        except:
            print("ERR ----- image")
        producer.kafka_producer_sync(topic="seloger-data", data=ad_data)
        driver.execute_script("window.close();")
        driver.switch_to.window(driver.window_handles[0])
    except:
        print('errrrrrrrrrrrrrrrrrrrrrrrrrrr')

        



def scrape_ads( driver):
    while True:
        print(driver.current_url)
        a= driver.find_elements(By.XPATH, '//a[@name="classified-link"]')
        b = []
        for count, i in enumerate(a):
            print(f'{count}/{len(a)} ---\n\n\n')
            url = i.get_attribute('href')
            if url not in b and 'seloger.com' in url:
                b.append(url)
                scrape_ad_url(url, driver)
        try:
            next_page = driver.find_element(By.XPATH, '//a[@data-testid="gsl.uilib.Paging.nextButton"]').get_attribute('href')
            driver.get(next_page)
            driver.implicitly_wait(10)
            solved = False
            while not solved:
                solved = isCaptcha(driver, next_page)
                if solved:
                    driver.get(url)
                else: 
                    break
                driver.implicitly_wait(5)
        except:
            break

        
def main_scraper():
    driver = WebDriver()
    global driverinstance
    driverinstance = driver.driver_instance
    url = "https://www.seloger.com/"
    # time.sleep(15)
    temp = accept_cookies(driverinstance, url)
    if not temp:
        print('ACCEPT COOKIES RETURNED FALSE')
        raise Exception('Captcha was not solved')
    driverinstance.find_element(By.XPATH, '//a[@class="c-quest-actions-search"]').click()
    scrape_ads( driverinstance)

