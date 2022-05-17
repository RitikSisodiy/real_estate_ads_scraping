import json
from string import ascii_lowercase
from urllib import response
import aiohttp
import asyncio
import fake_useragent
from urllib.parse import unquote
from requests import head, session
from requests_html import HTML,AsyncHTMLSession
try:
    from .GetFilter import getFilter
except:
    from GetFilter import getFilter
import ast
ua = fake_useragent.UserAgent(fallback='Your favorite Browser')
from kafka_publisher import KafkaTopicProducer,AsyncKafkaTopicProducer
producer = KafkaTopicProducer()
def getHeaders():
    return ua.random
async def fetch(session,url,proxy=None):
    # session = AsyncHTMLSession()
    # print(vars(session))
    headers = {
        # 'user-agent':getHeaders()
        'user-agent':'''Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36'''
    }
    # async with session.get(url,headers=headers,proxy=proxy) as responce:
    #     print(responce.status, url)
    #     if responce.status==200:
    #         doc = await responce.text()
    #         html = HTML(html=doc)
    #         return html
    #     else:
    #         return await fetch(session,url)
    try:
        if proxy:
            responce= await session.get(url,headers=headers,proxies=proxy)
        else:
            responce= await session.get(url,headers=headers)

        print(responce.status_code, url)
        if responce.status_code==200:
            return responce.html
        else:
            return await fetch(session,url,proxy)
    except:
        return await fetch(session,url,proxy)
def GetTotalPage(r):
    pagescript = r.find("script:contains('dataLayer')",first=True)  
    if pagescript:
        try:
            pagescript  = pagescript.text.split(";")[0].split("=")[1].strip()
            getdict = ast.literal_eval(pagescript)
            if getdict:
                totalresults = int(getdict[0]['gtm_nb_annonces'])
                pagesize = len(r.find(".ergov3-annonce"))
                totalpages = totalresults/pagesize
                totalpages = int(totalpages)+1 if int(totalpages)<totalpages else int(totalpages)
                return totalpages
            else:
                return 0 
        except:return 0
    else:return 0
async def scrape_ad(session,url,proxy,producer=producer):
    print("scrapin add ", proxy)
    r = await fetch(session,url,proxy)
    print("in last" ,url, proxy)
    if not r:return 0
    ad_data = {"image_urls": [], "3d_view": "", "location": "", "title": "", "price": "",
               "description": "", "location_url": "", "opening_timings": "", "features": [],
               "visting_form": "", "connectivity_index": "", "fiber_eligibility_rate": "",
               "vendor_information": {"vendor_type": "", "vendor_url": "", "vendor_name": "",
                                      "advertisement": "", "logo_url": "", "fb_url": "",
                                      "vendor_location": "", "opening_time": "","source":url}}
    try:
        r= r.find("#detail_h1")[0]
    except:
        pass
    ad_data['title']= r.find("#detail_h1",first=True).text  if r.find("#detail_h1",first=True) else ''
    ThreeD = r.find('#visiteVirtuelle',first=True)
    ad_data['3d_view']= ThreeD.attrs.get("href") if ThreeD else ""
    ad_data['location']= r.find(".fcbx_localisation2",first=True).text  if r.find(".fcbx_localisation2",first=True) else ''
    ad_data['location_url']= r.find("#detail_h1",first=True).attrs.get("url") if r.find("#detail_h1",first=True) else ''
    ad_data['price']= r.find("#autoprix",first=True).text if r.find("#autoprix",first=True) else ''
    ad_data['description']= r.find(".im12_txt_ann.im12_txt_ann_auto",first=True).text  if r.find(".im12_txt_ann.im12_txt_ann_auto",first=True) else ''
    ad_data['opening_timings']= r.find(".opt19_listlinks.opt19_listhor",first=True).text if r.find(".opt19_listlinks.opt19_listhor") else ''
    # ad_data['features']= r.find("#",first=True).text  if r.find("#",first=True).text else ''
    features = r.find('.crit-alignbloc') 
    for fea in features:
        for li in fea.find('li'):
            ad_data['features'].append(li.text.strip())

    ad_data['visting_form']= r.find(".im12_photosvoir",first=True).attrs.get("href") if r.find(".im12_photosvoir",first=True) else ''
    ad_data['connectivity_index']=  r.find(".qualiteconnexion_zonetiers div.indice_signif span")[0].text if r.find(".qualiteconnexion_zonetiers div.indice_signif span",first=True) else ''
    ad_data['fiber_eligibility_rate']= r.find("#detail_h1",first=True).text  if r.find("#detail_h1",first=True) else ''
    fburl = unquote(r.find("div.opt19_listlinks a[title=Facebook]",first=True).attrs.get("href").split("?url=")[1]) if r.find("div.opt19_listlinks a[title=Facebook]",first=True) else ''
    ad_data['vendor_information']['fb_url'] = fburl[:fburl.find('&')]
    ad_data['vendor_information']['vendor_url'] = r.find("div.detail_infosvendeu a", first= True).attrs.get("href") if r.find("div.detail_infosvendeu a", first= True) else ''
    ad_data['vendor_information']['vendor_name'] = r.find("#detail_infosvendeur",first=True).text if r.find("#detail_infosvendeur",first=True) else ''
    ad_data['vendor_information']['logo_url'] = r.find('div#detail_infosvendeur img',first=True).attrs.get('src') if r.find('div#detail_infosvendeur img',first=True) else ''
    # ad_data['vendor_information']['vendor_location'] = r.find('.opt19_listlinks')[3].text.strip() if r.find('.opt19_listlinks') else ''
    ad_data['vendor_information']['opening_time'] = r.find(".opt19_listhor",first=True).text.strip() if r.find(".opt19_listhor",first=True) else ''
    imgurl = r.find(".fcbx_photo5",first=True).attrs.get('url') if r.find(".fcbx_photo5",first=True) else ''
    if imgurl:
        await asyncio.sleep(1)
        r= await fetch(session,imgurl,proxy)
        imgs = r.find(".slides img")
        for img in imgs:
            ad_data['image_urls'].append(img.attrs.get('src'))
    ad_data = json.dumps(ad_data)
    with open("outputparu2.json",'a') as file:
        file.write(+"\n") 
    await producer.kafka_producer_async(topic="paruvendu-data_v1", data=ad_data)
    await asyncio.sleep(1)
async def scrape_pages(session,url,proxy=None,first = True,producer=producer):
    r = await fetch(session,url)
    iniurl = url
    if first:
        url = iniurl
        totalpage = GetTotalPage(r)
        proxies = []
        port = 9050
        for i in range(2,22):
            print(portno)
            portno= port+1
            prxy = {'http':f'socks4://127.0.0.1:{portno}','https':f'socks4://127.0.0.1:{portno}'}
            proxies.append(prxy)
        portno = 9050
        if totalpage>500:totalpage=500
        tasks = []
        count = 0
        pcount =0
        for i in range(1,totalpage+1):
            session = AsyncHTMLSession()
            proxy = proxies[count%len(proxies)]
            if "?" not in url:
                nexpageurl = f"{url}?p={i}"
            else:
                nexpageurl = f"{url}&p={i}"
            tasks.append(asyncio.ensure_future(scrape_pages(session,nexpageurl,proxy,False,producer=producer)))
            count +=1
            if count%len(proxies)==0 or count == totalpage:
                await asyncio.gather(*tasks)
                tasks = []
    if not r:return 0
    ads = r.find(".ergov3-annonce")
    tasks = []
    count = 0
    
    for ad in ads:
        # proxy = f"socks4://127.0.0.1:905{ip}"
        url = ad.find('a')[1].attrs.get('href')
        url = r._make_absolute(url)
        url = url.replace("https://example.org/",'https://www.paruvendu.fr/')
        tasks.append(asyncio.ensure_future(scrape_ad(session,url,proxy,producer=producer)))
        count +=1
        # if  or count == len(ads):
        await asyncio.gather(*tasks)
        tasks=[]
            # await asyncio.sleep(.5)
    # await asyncio.gather(*tasks)
    count = 0
        
async def scrape_rental_ads(mini_price, maxi_price, text,totalpages=None):
    """
     tt=1 sale house : Vente Maison
     tt=2 sale land : Vente terrain
     tt=5 location : rental

     ddlAffine = "text"
     px0  minimum price
     px1 maximum price
     codeINSEE
    """
    # async with aiohttp.ClientSession() as session:
    session = AsyncHTMLSession()
    rental_code = "5"
    total_pages = totalpages
    min_price = f"&px0={mini_price}"
    max_price = f"&px0={maxi_price}"
    codeINSEE = "&codeINSEE= "

    if len(text) > 3:
        text = f"&ddlAffine={text}"
        url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1{min_price}{max_price}&pa=FR&lol=0&ray=50{text} {codeINSEE},"
    else:
        url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&pa=FR&lol=0&ray=50"
    urls = getFilter(url)
    print(len(urls), " filters available do you wnat to continue")
    input()
    for url in urls:
        await asyncio.gather(asyncio.ensure_future(scrape_pages(session,url)))


async def scrape_sale_ads(mini_price, maxi_price, text):
    # async with aiohttp.ClientSession() as session:
        session = AsyncHTMLSession()
        producer= AsyncKafkaTopicProducer()
        await producer.start()
        rental_code = "1"
        min_price = f"&px0={mini_price}"
        max_price = f"&px0={maxi_price}"
        codeINSEE = "&codeINSEE= "
        if len(text) > 3 and mini_price > 0 and maxi_price > 0:
            text = f"&ddlAffine={text}"
            url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1{min_price}{max_price}&pa=FR&lol=0&ray=50{text} {codeINSEE},"
        else:
            url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&pa=FR&lol=0&ray=50{codeINSEE},"
        await  asyncio.gather(asyncio.ensure_future(scrape_pages(session,url,producer=producer)))
        await producer.close()





def main_scraper(paylaod):
    paylaod = {'text': 'house', 'min_price': 0.0, 'max_price': 0.0, 'city': 'string', 'rooms': 0,
               'real_state_type': 'rental'}
    if paylaod["real_state_type"] == "sale":
        print("sales code is working")
        asyncio.run(scrape_sale_ads(paylaod["min_price"], paylaod["max_price"], paylaod["text"]))

    elif paylaod["real_state_type"] == "rental":
        print("real code is working")
        asyncio.run(scrape_rental_ads(0, 0, ''))
async def main():
    # async with aiohttp.ClientSession() as session:
        session = AsyncHTMLSession()
        url = "https://www.paruvendu.fr/immobilier/"
        tasks  =  [ asyncio.ensure_future(fetch(session,url)) for i in range(0,100)]
        r = await asyncio.gather(*tasks)
        # print(r)
# asyncio.run(scrape_rental_ads(0, 0, ''))
