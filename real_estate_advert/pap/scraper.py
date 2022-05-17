import asyncio
from asyncio import tasks
from tkinter.tix import Tree
import aiohttp
import json
from unidecode import unidecode
# try:
#     from .getFilters import getFilter
# except:
#     from getFilters import getFilter

from requests import request
from kafka_publisher import KafkaTopicProducer
from requests_html import AsyncHTMLSession
producer = KafkaTopicProducer()

# get the source code of link
# async def fetch(session,url,retry=0,requesthtml=False):
#     print(url)
#     headers = {
#         "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
#     }
#     if requesthtml:
#         r = await session.get(url)
#         if r.status_code == 200:
#             return r.html
#         else:
#             retry+=1
#             if retry<=3:
#                 return await fetch(session,url,retry)
#             with open("error2.txt",'a',encoding='utf8') as file:
#                 file.write("no responce"+url+"\n")
#             return None
#     else:
#         async with session.get(url,headers=headers) as responce:
#             print(url,responce.status)
#             if responce.status == 200:
#                 doc = await responce.text()
#                 html = HTML(html=doc,url=responce.url, async_=True)
#                 return html
#             else:
#                 retry+=1
#                 if retry<=3:
#                     return await fetch(session,url,retry)
#                 with open("error2.txt",'a',encoding='utf8') as file:
#                     file.write("no responce"+url+"\n")
#                 return None
            

#         # assert response.status == 200
#         # data = await response.text()

async def fetch(session,url,retry=0):
    print(url)
    r = await session.get(url)
    if r.status_code == 200:
        return r
    else:
        retry+=1
        if retry<=3:
            return await fetch(session,url,retry)
        with open("error2.txt",'a',encoding='utf8') as file:
            file.write("no responce"+url+"\n")
        return None
    



async def getPropertyDetail(session,url,result):
    try:
        r= await fetch(session,url)
        if not r: return 0
        res= {}
        res['tel'] =r.html.find(".tel-wrapper .txt-indigo",first=True)
        res['tel'] = res['tel'].text if res['tel'] else None
        res['for'] = 'rent' if res['tel'] else 'sale'

        try:
            element = r.html.find('#carte_mappy')[0].attrs.get('data-mappy')
            # print(element)
            res['latitude-longitude'] = ",".join(json.loads(element).get('center'))
        except:
            # element = r.html.find('#mappy_detail')[0].attrs.get('data-mappy')
            # # print(element)
            # res['latitude-longitude'] = ",".join(json.loads(element).get('center'))
            pass
        adress = unidecode(r.html.find("div.margin-bottom-30")[0].text)
        res['address'] = adress[:adress.find("<br/>")]
        images = r.html.find("div.owl-carousel img")
        res['images'] = [data.attrs.get('src') for data in images]
        result['address'] = res
        T3dView = r.html.find("<iframe",first=True)
        T3dView = T3dView.attrs.get("src") if T3dView else "" 
        result["3d view"] = T3dView
        with open('final-5.json','a') as file:
            file.write(json.dumps(result)+"\n")
        producer.kafka_producer_sync(topic="test_pap2", data=result)
        # return result
    except Exception as e:
        with open("error2.txt",'a',encoding='utf8') as file:
            file.write(str(e)+url+"\n")
        pass

async def getProperty(session,url):
    r= await fetch(session,url)
    if not r: return 0 

    # cpageurl = r.html.find('meta[property="og:url"]',first=True).attrs.get('content')
    next = r.html.find('link[rel="next"]',first=True)
    properties  = r.html.find(".item-body") 
    propertyurl= []
    for data in properties:
        try:
            res = {}
            posturl = r.html._make_absolute(data.find('.item-title',first=True).attrs.get('href'))
            res['ad_url'] = posturl
            res['name'] = unidecode(data.find('.item-title span.h1', )[0].text)
            res['type'] = unidecode(posturl[posturl.find('/annonces/')+10:posturl.find('-')])
            res['price'] = unidecode(data.find('.item-title .item-price', )[0].text)
            res['tags'] = [unidecode(tag.text) for tag in data.find('.item-title ul.item-tags li')]
            res['description'] = unidecode(data.find('.item-description', )[0].text)
            propertyurl.append(asyncio.ensure_future(getPropertyDetail(session,posturl,res)))
            # res['address'] = Getlocation(baseurl+res['url'])
        except Exception as e:
            print(data.html)
            input('hello')
            with open("error2.txt",'a',encoding='utf8') as file:
                file.write(str(e)+url+"\n")
    if next:
        # print(next,next.attrs.get('content'))
        # input("hello")
        propertyurl.append(asyncio.ensure_future(getProperty(session,next.attrs.get('href'))))
        pass
    await asyncio.gather(*propertyurl)
    # return data
async def pap_scraper_run():

    # URL = 'https://www.pap.fr/annonce/vente-appartement-bureaux-divers-fonds-de-commerce-garage-parking-immeuble-local-commercial-local-d-activite-maison-mobil-home-multipropriete-peniche-residence-avec-service-surface-a-amenager-terrain-viager-a-partir-du-studio'
    # URL = 'https://www.pap.fr'
    # typesArr, piecesArr, area, price = get_input()
    # async with aiohttp.ClientSession() as session:
    urls = getFilter('rental')
    session = AsyncHTMLSession()
    tasks = []
    for url in urls:
        tasks.append(asyncio.ensure_future(getProperty(session,url)))
    await asyncio.gather(*tasks)
    # driver.get(URL)

    # # select the filters
    # # select_types(driver, typesArr)
    # # select_pieces(driver, piecesArr)
    # # input_area(driver, area)
    # # input_price(driver, price)

    # # search(driver)

    # WAIT_TIME = 2
    # SCROLL_COUNTER = 100


    # link = None
    # t = time.time()
    # last_height = driver.execute_script("return document.body.scrollHeight")
    # while True:
    #     # cancel the email confimation dialog pop-us
    #     try:
    #         WebDriverWait(driver, 0.5).until(
    #             EC.visibility_of_element_located(
    #                 (By.CLASS_NAME, 'btn-fermer-dialog '))
    #         ).click()
    #     except BaseException:
    #         pass
    #     if time.time() - t > WAIT_TIME:
    #         html = driver.page_source
    #         res = BeautifulSoup(html, 'html.parser')
    #         links = transform(res)

    #         # get links after the prev
    #         i = 0 if link is None else links.index(link) + 1
    #         print(len(links))
    #         for i in range(i, len(links)):
    #             link = links[i]
    #             data = ad_info(link, driver)
    #             producer.kafka_producer_sync(topic="pap-data", data=data)

    #         t = time.time()

    #         scroll_down(driver)
    #         # Calculate new scroll height and compare with last scroll height
    #         new_height = driver.execute_script(
    #             "return document.body.scrollHeight")
    #         if new_height == last_height:
    #             SCROLL_COUNTER -= 1
    #             if SCROLL_COUNTER == 0:
    #                 print('Can not scroll more')
    #                 break
    #         last_height = new_height
    # driver.close()

def pap_scraper():
    asyncio.run(pap_scraper_run())