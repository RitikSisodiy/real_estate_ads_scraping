from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import json
import time

from kafka_publisher import KafkaTopicProducer


# get the source code of link
def extract_html(driver, URL):
    # Open link on new window -> get the source code -> close the code
    driver.execute_script("window.open('');")
    WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
    driver.switch_to.window(driver.window_handles[1])
    driver.get(URL)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.execute_script("window.close();")
    driver.switch_to.window(driver.window_handles[0])
    return soup


# reterive all the information of a property from a page
def ad_info(URL, driver=None):
    # defining the variables that will be extracted
    name = ''
    price = 0
    desc = ''
    tags = []
    images = []
    tel = 0
    date = 0

    # get the source code from URL
    soup = extract_html(driver, URL)

    # Extract the requried variables
    try:
        div = soup.find('div', class_='item-description')
    except Exception as e:
        print('error ->>>>>> ', e)
    try:
        name = soup.find('h1', class_='item-title').text.strip()
    except Exception as e:
        print('error ->>>>>> name not found')
    try:
        price = soup.find('span', 'item-price').text.strip()
    except Exception as e:
        print('error ->>>>>> price not found')
    try:
        desc = div.find('p').text.strip()
    except Exception as e:
        print('error ->>>>>> description not found')
    try:
        uls = div.find('ul')
        for i in uls.find_all('li'):
            tags.append(i.text.strip())
    except Exception as e:
        print('error ->>>>>> tags not found')
    try:
        image_divs = soup.find('div', class_='owl-thumbs')
        if image_divs is not None:
            links = image_divs.find_all('img')
            for image in links:
                images.append(image['src'])
    except Exception as e:
        print('error ->>>>>> images not found')

    try:
        tel = soup.find('p', class_='tel-wrapper')
    except Exception as e:
        print('error ->>>>>> phone number not found')
    # 3d view
    try:
        view_link = None
        view_links = div.find_all('a', class_='btn')
        for i in view_links:
            if i['href'].startswith('https'):
                view_link = i['href']
    except Exception as e:
        print('error ->>>>>> 3d link not found')

    try:
        date = soup.find('p', class_='item-date').text.strip()
        date = ' '.join(date.split(' ')[-3:])
    except Exception as e:
        print('error ->>>>>> date not found')

    # create a dictionary
    place = {
        'name': name,
        'price': price,
        'images': images,
        'description': desc,
        'tags': tags,
        'tel': 0 if tel is None else tel.find(
            'span',
            class_='txt-indigo').text.strip(),
        '3d_view': '' if view_link is None else view_link,
        'date': date}
    return place


# get all the links of properties from source code of page
def transform(soup):
    divs = soup.find_all('div', class_='item-body')
    links = []
    for item in divs:
        link = item.find('a')['href']
        # don't include the links that lead to other pages or that are ads not
        # properites
        if 'annonces' not in link or link.startswith('https'):
            continue
        links.append('https://www.pap.fr' + link)
    return links


# scrolling down
def scroll_down(driver, SCROLL_PAUSE_TIME=2):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(SCROLL_PAUSE_TIME)
    new_height = driver.execute_script("return document.body.scrollHeight")


# get information form json and append new one
def update_json(filename, data):
    try:
        with open(filename, "r") as file:
            json_data = json.load(file)
    except BaseException:
        # if file doesn't exists
        with open(filename, "w") as file:
            json_data = json.dump([], file)

        with open(filename, "r") as file:
            json_data = json.load(file)

    # add data and write it to the file
    json_data.append(data)
    with open(filename, "w") as file:
        json.dump(json_data, file)


# get the list of selectable options from class name or xpath
def get_select_list(driver, class_name, xpath=False):
    return driver.find_element(
        By.CLASS_NAME if not xpath else By.XPATH,
        class_name).find_element(
        By.CLASS_NAME,
        'optWrapper').find_element(
        By.TAG_NAME,
        'ul').find_elements(
        By.TAG_NAME,
        'li')


# select the options of "types de bien"
def select_types(driver, typeArr):
    try:
        t = driver.find_element(By.XPATH, '//p[@title=" Types de bien"]')
        t.click()
        types = get_select_list(driver, 'sumo_typesbien')
        for i in types:
            if i.text.strip() in typeArr:
                print([i.text.strip()])
                i.click()
            # else:
            #     print([i.text.strip(), typeArr])

        t.click()
    except Exception as e:
        print(f'ERROR AT SELECT TYPES ----> {e}')


# select the options of "Pieces"
def select_pieces(driver, pieceArr):
    s = driver.find_element(By.XPATH, '//p[@title=" Pièces"]')
    s.click()
    pieces = driver.find_elements(By.XPATH, '//li[@class="opt"]')
    for i in pieces:
        if i.text.strip() in pieceArr:
            print([i.text.strip()])
            i.click()
    s.click()


# Enter the value of max price
def input_price(driver, p=-1):
    if p < 0:
        return
    price = driver.find_element(By.ID, 'surface_min')
    price.send_keys(p)


# Enter the value of minimum area
def input_area(driver, a=-1):
    if a < 0:
        return
    area = driver.find_element(By.ID, 'prix_max')
    area.send_keys('1234')


# search for the links
def search(driver):
    submit = driver.find_element(
        By.XPATH, "//a[@href='#dialog_creer_une_alerte']")
    submit.click()
    # cencel the email confimation dialog
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="submit-sans-creer-alerte"]'))
    ).click()


# Hard-coded input values
def get_input():
    typesArr = ['Maison', 'Appartement', 'Terrain',
                'Garage, parking', 'Surfaces diverses']
    piecesArr = ['Studio', '3 pièces', '4 pièces', '5 pièces']
    area = 100
    price = -1
    return typesArr, piecesArr, area, price


def get_types_complete_list(driver):
    types_list = []
    t = driver.find_element(By.XPATH, '//p[@title=" Types de bien"]')
    t.click()
    types = get_select_list(driver, 'sumo_typesbien')
    for i in types:
        if i.text.strip() != '':
            types_list.append(i.text.strip())
    return types_list


def get_pieces_complete_list(driver):
    pieces_list = []
    s = driver.find_element(By.XPATH, '//p[@title=" Pièces"]')
    s.click()
    pieces = driver.find_elements(By.XPATH, '//li[@class="opt"]')
    for i in pieces:
        pieces_list.append(i.text.strip())
    return pieces_list


def main_scraper():
    # URL = 'https://www.pap.fr/annonce/vente-appartement-bureaux-divers-fonds-de-commerce-garage-parking-immeuble-local-commercial-local-d-activite-maison-mobil-home-multipropriete-peniche-residence-avec-service-surface-a-amenager-terrain-viager-a-partir-du-studio'
    # URL by selecting only types de bien
    # URL = 'https://www.pap.fr/annonce/vente-appartement-bureaux-divers-fonds-de-commerce-garage-parking-immeuble-local-commercial-local-d-activite-maison-mobil-home-multipropriete-peniche-residence-avec-service-surface-a-amenager-terrain-viager'
    # only houses
    # URL = 'https://www.pap.fr/annonce/vente-maisons'
    # only appartment
    # URL = 'https://www.pap.fr/annonce/vente-appartements'
    WAIT_TIME = 2
    SCROLL_COUNTER = 0
    producer = KafkaTopicProducer()
    FILE_NAME = 'filters.json'
    SCROLS = 0
    TOTAL = 0
    URL = 'https://www.pap.fr'

    options = webdriver.ChromeOptions()
    # options.addar
    options.add_argument("--headless")
    options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0")

    driver = webdriver.Chrome(options=options, executable_path="chromedriver")
    # driver = webdriver.Chrome()
    driver.get(URL)

    types_list = get_types_complete_list(driver)
    pieces_list = get_pieces_complete_list(driver)

    for typeDeBien in types_list:
        for pieces in pieces_list:
            # typesArr, piecesArr, area, price = get_input()

            # print([i], [j])
            driver.delete_all_cookies()
            driver.get(URL)

            # # select the filters
            driver.implicitly_wait(5)
            select_types(driver, [typeDeBien])
            select_pieces(driver, [pieces])
            # select_types(driver, typesArr)
            # select_pieces(driver, piecesArr)
            # input_area(driver, area)
            # input_price(driver, price)

            # # submit and search the results
            search(driver)

            link = None
            t = time.time()
            print(f'{t} ----------------------------------------------------------\n\n\n\n ')
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                # cancel the email confimation dialog pop-us
                try:
                    WebDriverWait(driver, 0.5).until(
                        EC.visibility_of_element_located(
                            (By.CLASS_NAME, 'btn-fermer-dialog '))
                    ).click()
                except BaseException:
                    pass
                    # if time.time() - t > WAIT_TIME:

                    # t = time.time()

                    scroll_down(driver)
                    SCROLL_COUNTER += 1
                    # Calculate new scroll height and compare with last scroll height
                    new_height = driver.execute_script(
                        "return document.body.scrollHeight")
                    if new_height == last_height:
                        # SCROLL_COUNTER -= 1
                        # if SCROLL_COUNTER == 0:
                        # print('Can not scroll more')
                        # break
                        # if not EC.visibility_of_element_located((By.CLASS_NAME, "loader")):
                        #     break
                        if not driver.find_element(By.ID, 'loader-next').is_displayed():
                            print('loader is not there')
                            html = driver.page_source
                            res = BeautifulSoup(html, 'html.parser')
                            links = transform(res)

                            # get links after the prev
                            i = 0 if link is None else links.index(link) + 1
                            # print(len(links))
                            for i in range(i, len(links)):
                                link = links[i]
                                TOTAL += 1
                                print(f'\n\n{TOTAL} --- fetching data from {link}')
                                data = ad_info(link, driver)
                                producer.kafka_producer_sync(topic="pap-data", data=data)
                                # update_json(FILE_NAME, data)
                                print(data)
                            print(SCROLL_COUNTER)
                            break
                        print('must have a loader')

                    last_height = new_height
