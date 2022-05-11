from asyncio import tasks
import imp
from json import tool
import json
import aiohttp
import asyncio
import fake_useragent
from requests import session
from requests_html import HTML
import ast
ua = fake_useragent.UserAgent(fallback='Your favorite Browser')

def getHeaders():
    return ua.random
async def fetch(session,url):
    headers = {
        'user-agent':getHeaders()
    }
    async with session.get(url,headers=headers) as responce:
        print(responce.status, url)
        if responce.status==200:
            doc = await responce.text()
            html = HTML(html=doc,url= responce.url)
            return html
        else:
            return await fetch(session,url)
def GetTotalPage(r):
    pagescript = r.html.find("script:contains('dataLayer')",first=True)  
    if pagescript:
        try:
            pagescript  = pagescript.text.split(";")[0].split("=")[1].strip()
            getdict = ast.literal_eval(pagescript)
            if getdict:
                totalresults = getdict[0]['gtm_nb_annonces']
                pagesize = len(r.find(".ergov3-annonce"))
                totalpages = totalresults/pagesize
                totalpages = int(totalpages)+1 if int(totalpages)<totalpages else int(totalpages)
                return totalpages
            else:
                return 0 
        except:return 0
    else:return 0
async def scrape_ad(session,url):
    r = await fetch(session,url)
    if not r:return 0
    ad_data = {"image_urls": [], "3d_view": "", "location": "", "title": "", "price": "",
               "description": "", "location_url": "", "opening_timings": "", "features": [],
               "visting_form": "", "connectivity_index": "", "fiber_eligibility_rate": "",
               "vendor_information": {"vendor_type": "", "vendor_url": "", "vendor_name": "",
                                      "advertisement": "", "logo_url": "", "fb_url": "",
                                      "vendor_location": "", "opening_time": ""}}
    r= r.find("#detail_h1")[0]
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
    ad_data['vendor_information']['fb_url'] = r.find("div.opt19_listlinks a",first=True).attrs.get("href") if r.find("div.opt19_listlinks a",first=True) else ''
    ad_data['vendor_information']['vendor_url'] = r.find("div.detail_infosvendeu a", first= True).attrs.get("href") if r.find("div.detail_infosvendeu a", first= True) else ''
    ad_data['vendor_information']['vendor_name'] = r.find("#detail_infosvendeur",first=True).text if r.find("#detail_infosvendeur",first=True) else ''
    ad_data['vendor_information']['logo_url'] = r.find('div#detail_infosvendeur img',first=True).attrs.get('src') if r.find('div#detail_infosvendeur img',first=True) else ''
    ad_data['vendor_information']['vendor_location'] = r.find('.opt19_listlinks')[4].text.strip() if r.find('.opt19_listlinks') else ''
    ad_data['vendor_information']['opening_time'] = r.find(".opt19_listhor",first=True).text.strip() if r.find(".opt19_listhor",first=True) else ''
    imgurl = r.find(".fcbx_photo5",first=True).attrs.get('url') if r.find(".fcbx_photo5",first=True) else ''
    if not imgurl:return 0
    r= await fetch(session,imgurl)
    imgs = r.find(".slides.img")
    for img in imgs:
        ad_data['image_urls'].append(img.attrs.get('src'))
    with open("outputparu.json",'a') as file:
        file.write(json.dumps(ad_data)+"\n") 

async def scrape_pages(session,url,first = True):
    r = await fetch(session,url)
    if not r:return 0
    ads = r.find(".ergov3-annonce")
    tasks = []
    for ad in ads:
        url = ad.find('a')[1].attrs.get('href')
        url = r._make_absolute(url)
        tasks.append(asyncio.ensure_future(scrape_ad(session,url)))
    await asyncio.gather(*tasks)
    if first:
        totalpage = GetTotalPage(r)
        
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
    await asyncio.gather(asyncio.ensure_future(scrape_pages(session,url)))


async def scrape_sale_ads(mini_price, maxi_price, text):
    rental_code = "1"
    min_price = f"&px0={mini_price}"
    max_price = f"&px0={maxi_price}"
    codeINSEE = "&codeINSEE= "
    if len(text) > 3 and mini_price > 0 and maxi_price > 0:
        text = f"&ddlAffine={text}"
        url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1{min_price}{max_price}&pa=FR&lol=0&ray=50{text} {codeINSEE},"
    else:
        url = f"https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt={rental_code}&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&pa=FR&lol=0&ray=50{codeINSEE},"
    await  asyncio.gather(asyncio.ensure_future(scrape_pages(session,url)))





def main_scraper(paylaod):
    paylaod = {'text': 'house', 'min_price': 0.0, 'max_price': 0.0, 'city': 'string', 'rooms': 0,
               'real_state_type': 'rental'}
    if paylaod["real_state_type"] == "sale":
        print("sales code is working")
        scrape_sale_ads(paylaod["min_price"], paylaod["max_price"], paylaod["text"])

    elif paylaod["real_state_type"] == "rental":
        print("real code is working")
        scrape_rental_ads(paylaod["min_price"], paylaod["max_price"], paylaod["text"], driverinstance)
async def main():
    async with aiohttp.ClientSession() as session:
        url = "https://www.paruvendu.fr/immobilier/"
        tasks  =  [ asyncio.ensure_future(fetch(session,url)) for i in range(0,100)]
        r = await asyncio.gather(*tasks)
        # print(r)
asyncio.run(main())