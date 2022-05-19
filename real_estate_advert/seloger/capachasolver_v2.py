from ast import While
from xml.dom import ValidationErr
from selenium import webdriver
import time
import sys
import undetected_chromedriver as uc
import traceback
from selenium.webdriver import DesiredCapabilities
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from AudioToText import GetAudioToNumber
from urllib.parse import unquote
import json
import os
headless = input('do you want to run headless y/n')
if headless == "y" or headless=="Y" :headless=True
else: headless=False
currentpath = os.getcwd()
chromedirver = ChromeDriverManager().install()
try:
    vpn = sys.argv[1]
    ip = sys.argv[2]
except:
    vpn = False 
    ip=None
def saveCookies(cookies):
    cookie = ""
    for data in cookies:
        if data['name'] == "atuserid":
            pass
        else:
            cookie += data['name']+"="+unquote(data['value'])+"; "
    final  = ""
    with open("cookies.json",'r') as file:
        data = file.readlines()
        if len(data)>=8:
            for i in range(3,7):
                final += data[i]
    if final:
        with open("cookies.json",'w') as file:
            file.write(final)
    with open("cookies.json",'a') as file:
        file.write(cookie+"\n")
# def getdriver(proxy=None):
#     options = Options()
#     options.add_argument('--no-sandbox')
#     options.add_argument('--start-maximized')
#     options.add_argument('--single-process')
#     if headless:
#         options.add_argument('--headless')
#     options.add_argument('--disable-dev-shm-usage')
#     options.add_argument('window-size=1920x1080')
#     options.add_argument("--incognito")
#     options.add_argument('--disable-blink-features=AutomationControlled')
#     # options.add_argument('--disable-blink-features=AutomationControlled')
#     options.add_argument("--mute-audio")
#     options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36")
#     options.add_experimental_option('useAutomationExtension', False)

#     options.add_experimental_option("excludeSwitches", ["enable-automation"])
#     if proxy:
#         options.add_argument('--proxy-server=%s' % proxy)
#     capabilities = DesiredCapabilities.CHROME.copy()
#     capabilities['acceptSslCerts'] = True 
#     capabilities['acceptInsecureCerts'] = True
#     driver = webdriver.Chrome(f"{currentpath}/chromedriver",options=options,desired_capabilities=capabilities)
#     # driver = webdriver.Chrome('./chromedriver',options=options)
#     return driver


# def getdriver(proxy=None):
#     #! Dont change anything
#     chromeOptions = uc.ChromeOptions()
#     # chromeOptions.add_argument("--no-sandbox");
#     # chromeOptions.add_argument("--disable-dev-shm-usage");
#     # chromeOptions.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36")

#     # # chromeOptions.add_argument("--no-sandbox")
#     # # chromeOptions.add_argument("--headless")
#     # chromeOptions.add_argument("--disable-infobars")
#     # chromeOptions.add_argument("enable-automation");
#     # chromeOptions.headless=True
#     if headless:
#         chromeOptions.add_argument("--headless");
#     # # chromeOptions.add_argument("--window-size=1920,1080");
#     # chromeOptions.add_argument("--no-sandbox");
#     # chromeOptions.add_argument("--disable-extensions");
#     # chromeOptions.add_argument("--dns-prefetch-disable");
#     # chromeOptions.add_argument("--disable-gpu");
#     # # chromeOptions.add_argument("--disable-gpu")
#     # chromeOptions.add_argument("--disable-extensions")
#     # chromeOptions.add_argument("--mute-audio")
#     # chromeOptions.add_experimental_option(
#     #     'excludeSwitches', ['enable-logging'])
#     # chromeOptions.add_experimental_option("prefs", {"profile.default_content_setting_values.media_stream_mic": 2,
#     #                                                 "profile.default_content_setting_values.media_stream_camera": 2,
#     #                                                 "profile.default_content_setting_values.geolocation": 2,
#     #                                                 "profile.default_content_setting_values.notifications": 2
#     #                                                 })
#     if proxy:
#         chromeOptions.add_argument('--proxy-server=%s' % proxy)

#     driver = uc.Chrome(options=chromeOptions)
#     return driver

def getdriver(proxy=None):
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--start-maximized')
    options.add_argument('--single-process')
    if headless:
        options.add_argument('--headless')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--incognito")
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("--mute-audio")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36")
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    if proxy:
        options.add_argument('--proxy-server=%s' % proxy)
    driver = webdriver.Chrome(chromedirver,options=options)
    return driver

def solveCapcha(driver):
    a = 6
    count = 0
    while True:
        try:
            # count +=1
            # if vpn=="tor":
            #     print("working with tor")
            #     driver = getdriver(proxy="socks4://127.0.0.1:9050")
            # # driver = getdriver(proxy="http://41.65.236.44:1981")
            # # driver = getdriver(proxy="socks4://161.82.252.36:4153")
            # elif vpn=='notor':
            #     print("working without tor")
            #     driver = getdriver()
            # else:
            #     pr = ip
            #     print(pr)
            #     driver = getdriver(proxy = pr)
            # url ='''https://www.leboncoin.fr/'''
            # url = "https://www.seloger.com/"
            # driver.get(url) 
            pagetitle = driver.title
            print("this is the page tile ", pagetitle)
            if pagetitle == 'Petites annonces immobilières | 1er site immobilier français | Portail immo':
                cookies = driver.get_cookies()
                saveCookies(cookies)
                print(f"{count} cookies saved")
                # driver.close()
                return "solved"
            driver.switch_to.frame(0)
            wait = WebDriverWait(driver, 25)
            statustitle = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="captcha-container"]/div[3]/div/div[1]')))
            print(statustitle.text)
            wait = WebDriverWait(driver, 25)
            # verify = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="1e505deed3832c02c96ca5abe70df9ab"]/div/div[2]/div[1]')))
            verify = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'geetest_radar_tip')))
            time.sleep(2)
            verify.click()
            audiobutton = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/div/div[2]/div/a[4]')))
            time.sleep(2)
            audiobutton.click()
            time.sleep(1)
            # audiosrc = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/div/div/div/div[2]/div[2]/audio')))
            solved= False
            lastsrc = '1'
            while not solved:
                src = ""
                while not src or src==lastsrc:
                    if src == lastsrc:
                        wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/div[2]'))).click()
                        time.sleep(1)
                    audiosrc = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/div/div/div/div[2]/div[2]/audio')))
                    time.sleep(1)
                    src = audiosrc.get_attribute("src")
                lastsrc = src
                num = GetAudioToNumber(src)
                inputbutton = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/div/div/div/div[2]/div[3]/input')))
                inputbutton.send_keys(num)
                submit = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/div/div/div/div[2]/div[4]')))
                time.sleep(1)
                submit.click()
                try:
                    if len(num)==6:
                        print("in try success chekc")
                        return "solved"
                        succes = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="1e505deed3832c02c96ca5abe70df9ab"]/div/div[2]/div[2]/div/div[2]/span[1]')))
                        # solved = True
                    else:
                        print("in try success else")
                        # return False
                        raise ValidationErr
                except:
                    # return False
                    try:
                        print("in try except")
                        wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/div[2]'))).click()
                        time.sleep(1)
                    except:solved =True
                if solved == True:
                    title = "leboncoin, site de petites annonces gratuites"
                    print("cheking title :",title)
                    try:
                        wait.until(EC.title_is(title))
                    except:
                        driver.get(url)
                        wait.until(EC.title_is(title))
                    cookies = driver.get_cookies()
                    saveCookies(cookies)
                    print(f"{count} cookies saved")
                    driver.close()
                    return "solved"
                    break
                    time.sleep(100)
        except Exception as e:
            print(traceback.format_exc())
            print(e,"++++++++++++++++++++++++++++==")
            driver.close()
            time.sleep(5)
            pass
        

def isCaptcha(driver, url):
    solved = False
    count =0 
    isCaptchaPresent =False
    while count <= 11:
        count += 1
        try: 
            frames = driver.find_elements(By.TAG_NAME, 'iframe')
            for counter, i in enumerate(frames):
                if 'geo.captcha-delivery.com' in i.get_attribute('src'):
                    print('solving .........')
                    isCaptchaPresent = True
                    solved = solveCapcha(driver)
                    if solved == "solved":
                        print('Captcha must have been solved')
                        return True
                    else:
                        driver.get(url)
                        
            else: 
                break
        except Exception as e:
            print('Captcha Error')
            continue
    return isCaptchaPresent

if __name__ == "__main__":
    def getStatus():
        with open("status.txt",'r') as file:
            return file.read()
    while True:
        while True:
            time.sleep(5)
            status = getStatus()
            print(status,"this is status")
            if status == "403":
                solveCapcha()
                time.sleep(20)
                
                