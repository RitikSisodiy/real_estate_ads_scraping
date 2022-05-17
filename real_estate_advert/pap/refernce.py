from cgitb import text
from unittest import result
from requests_html import AsyncHTMLSession
import asyncio
from unidecode import unidecode
import time
import json
# url = "http://quotes.toscrape.com/random"
url= ["https://www.pap.fr/plan-du-site"]
# url= "https://www.pap.fr/annonce/annonce-vente-a-partir-du-studio"
# url ="https://www.pap.fr/annonce/vente-immobiliere-a-partir-du-studio"
# pagesize = 100
# url = ""
status = 1
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
    
        # assert response.status == 200
        # data = await response.text()
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
        return result
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
    data =await asyncio.gather(*propertyurl)
    return data
    # else:
    #     print("page is over last page url",url)
    # print(next,'this si ')
    # page = cpageurl[cpageurl.rfind("-")+1:]
    # urlpage = url[url.rfind("-")+1:]
    # if not page.isnumeric():
    #     page=1
    # print(r,page,urlpage)
    # return r

async def main():
    session = AsyncHTMLSession()
    # url = "https://www.pap.fr/plan-du-site/vente"
    url = "https://www.pap.fr/plan-du-site/"
    r = await fetch(session,url)
    extraurl = [
        'https://www.pap.fr/annonce/locations-studio',
        'https://www.pap.fr/annonce/vente-immobiliere-studio',
        'https://www.pap.fr/annonce/locations-loft',
        'https://www.pap.fr/annonce/vente-lofts-3',
        'https://www.pap.fr/annonce/locations-parking',
        'https://www.pap.fr/annonce/garage-a-vendre',
        'https://www.pap.fr/annonce/viager-viager',
    ]
    # urls = r.html.find(".dash-list:nth-child(10) a , .margin-top-20 a , .dash-list:nth-child(7) a")
    # urls = r.html.find(".list-big+ .list-big a , .list-big:nth-child(8) a , .list-big:nth-child(5) a")
    urls = r.html.find(".list-big:nth-child(8) a , .list-big+ .list-big a , .list-big:nth-child(5) a")
    tasks = [asyncio.ensure_future(getProperty(session,r.html._make_absolute(link.attrs.get('href')))) for link in urls]
    for d in extraurl: tasks.append(getProperty(session,d))
    data = await asyncio.gather(*tasks)
    return data
stat = time.time()
asyncio.run(main())
print(str(time.time()-stat)+" seconds")