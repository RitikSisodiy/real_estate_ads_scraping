from datetime import datetime,timedelta
import traceback
import asyncio
import os,time,concurrent.futures
from requests_html import HTML
import json,requests,settings

from saveLastChaeck import saveLastCheck
from . getGeolocation import Fetch
from HttpRequest.requestsModules import HttpRequest
from .parser import ParseParuvendu
from HttpRequest.uploader import AsyncKafkaTopicProducer
geoob = Fetch()
headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"
    }
pagesize  = 100 # maxsize is 100
import json
import time
website = "paruvendu.fr"
kafkaTopicName = settings.KAFKA_PARUVENDU
commanTopicName =settings.KAFKA_COMMON_PATTERN
commonIdUpdate = f"activeid-{website}"
def GetUrlFilterdDict(url):
    sdata = url.split("?")
    data = sdata[1].split("&")
    baseurl = sdata[0]
    res = {}
    for da in data:
        key,val = da.split("=")
        res[key] = val
    return res,baseurl
def getUrl(baseurl,dic):
    final = baseurl+"?" if "?" not in baseurl else baseurl
    count = 0
    finalli = []
    for key,value in dic.items():
        finalli.append(f"{key}={value}")
    finalli = "&".join(finalli)
    final += finalli
    return final
def getTotalResult(params,url):
    totalres = {
        'itemsPerPage':1,
        'showdetail':0
    }
    params.update(totalres)
    r = s.get(url,params=params)
    return int(r.json()["feed"]["@totalResults"])
def getMaxPrize(params,url):
    # dic,burl = GetUrlFilterdDict(url)
    # dic["ajaxAffinage"] = 0
    # dic["ddlTri"] = "prix_seul"
    # dic["ddlOrd"] = "desc"
    prizefilter = {
        "sortOn":"prix",
        "sortTo":"DESC",
        "itemsPerPage":1
    }
    params.update(prizefilter)
    r = s.get(url,params=params)
    prize = r.json()["feed"]["row"][0]['price']
    maxprice = ''
    for c in prize:
        try:maxprice+=f"{int(c)}"
        except:pass
    if maxprice:return int(maxprice)
from requests_html import HTMLSession 
s= HTMLSession()
s.headers ={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"}
 
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?nbp=0&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&ddlFiltres=nofilter"
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?nbp=0&tt=5&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&ddlFiltres=nofilter"
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt=5&at=1&nbp0=99&pa=FR&lo=&codeINSEE="
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt=1&at=1&nbp0=99&pa=FR&lo=&codeINSEE="
def getFilter(session,params,**kwargs):
    dic,baseurl = params , "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"
    # url = getUrl(baseurl,dic)
    url = baseurl
    totalresult =getTotalResult(dic,baseurl)
    acres = totalresult
    fetchedresult = 0
    iniinterval = [0,1000]
    filterurllist = ''
    finalresult = 0
    maxresult = 12500 # we can only get 12500 result by one filter url
    maxprice = getMaxPrize(params,baseurl)
    if totalresult>=maxresult:
        while iniinterval[1]<=maxprice:
            dic['filters[P5M0]'],dic['filters[P5M1]'] = iniinterval
            totalresult = getTotalResult(dic,baseurl)
            if totalresult < maxresult and maxresult-totalresult<=2000:
                print("condition is stisfy going to next interval")
                print(iniinterval,">apending")
                # filterurllist.append(iniinterval)
                # filterurllist+=json.dumps(dic)+":\n"
                startCrawling(session,[dic],**kwargs)
                print(filterurllist)
                iniinterval[0] = iniinterval[1]+1
                iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
                finalresult +=totalresult
            elif maxresult-totalresult> 200:
                print("elif 1")
                last = 10
                iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
            elif totalresult>maxresult:
                print("elif 2")
                last = -10
                iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
            print(totalresult,"-",maxresult)
        print(iniinterval,">apending")
        print(filterurllist)
        # filterurllist+=json.dumps(dic)
        startCrawling(session,[dic],**kwargs)
        finalresult +=totalresult
    finallsit = [json.loads(d) for d in filterurllist.split(":\n")]
    print(finallsit)
    # paramslist = []
    # for par in finallsit:
    #     params['filters[P5M0]'],params['filters[P5M1]'] = par
    #     paramslist.append(params)
    print(f"total result is : {acres} filtered result is: {finalresult}")
    print(filterurllist)
    # time.sleep(10)
    return finallsit
cpath =os.path.dirname(__file__) or "."
url = "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"
params = {
        'ver':'4.1.4',
        'itemsPerPage':'12',
        'mobId':'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd',
        'p':'1',
        'sortOn':'dateMiseEnLigne',
        'sortTo':'DESC',
        'key':'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh',
    }
def fetch(session,url,params = None,method="get",Json=True,retry=0,**kwargs):
    # if params:
    #     query_string = urllib.parse.urlencode( params )
    #     url += "?"+query_string 
    if retry>3:
        return None
    retry+=1
    try:
        res = session.fetch(url,params=params)
    except Exception as e:
        time.sleep(3)
        return fetch(session,url,params,method,Json=Json,retry=retry,**kwargs)
    try:
        print(res.status_code)
        if Json:response = res.json()
        else:response=res
    except: return fetch(session,url,params,method,Json=Json,retry=retry,**kwargs)
    return response

def savedata(resjson,**kwargs):
    resstr = ''
    ads = resjson['feed']["row"]
    producer = kwargs["producer"]
    if kwargs.get("onlyid"):
        now = datetime.now()
        ads = [{"id":ad.get("id"), "last_checked": now.isoformat(),"available":True} for ad in ads]
        producer.PushDataList_v1(commonIdUpdate,ads)
    else:
        producer.PushDataList(kafkaTopicName,ads)
        ads = [ParseParuvendu(ad) for ad in ads]
        ads = asyncio.run(geoob.getAllgeo(ads))
        producer.PushDataList(commanTopicName,ads)
    # for ad in ads:
    #     resstr += json.dumps(ad)+"\n"
    # with open("output.json",'a') as file:
    #     file.write(resstr)
    # print('saved data')s
def startCrawling(session,filterParamList,**kwargs):
    for param in filterParamList:
        if kwargs.get("onlyid"):param['showdetail'] = 0
        else:param['showdetail'] = 1
        param["itemsPerPage"] = pagesize
        data = fetch(session,url,param)
        # data= json.load(res)
        savedata(data,**kwargs)
        totalres = int(data["feed"]["@totalResults"])
        totalpage = totalres/pagesize
        totalpage = int(totalpage) if totalpage==int(totalpage) else int(totalpage)+1
        print(totalres,param)
        data = []
        print(totalpage,"this is total pages")
        totaldata = 0
        for i in range(2,totalpage+1):
            param['p'] = i
            # print(param)
            parstItems(session,param.copy(),page=i,**kwargs)
        # data = asyncio.gather(*tasks)  
        totaldata = 0
        for d in data:
            results = len(d['feed']["row"])
            print(results,"this is len of rows")
            totaldata += results
            savedata(d,**kwargs)
        print(totaldata)
def parstItems(session,param,page=None,**kwargs):
    param.update({"p":page})
    data = fetch(session,url,param)
    # print(param['p'])
    # data = json.load(res)
    savedata(data,**kwargs)
    return data
def CheckId(id):
    furl  = f"https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list?paId={id}&showdetail=1&mobId=dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd&key=lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh"
        # print(furl)
    r = requests.get(furl,headers=headers)
    # print(r)
    data = r
    print(len(data["feed"]["row"]))
    if data and len(data["feed"]["row"])==1:
        return True
    else:return False
def main(adsType = "",onlyid= False):
    # catid info
    # IVH00000 is for  Vente immobilier 
    # ILH00000 is for Location immobilier
    if adsType == "sale":
        catid = "IVH00000"
    else:
        catid = "ILH00000"
    params["catId"] = catid
    params["filters[_R1]"] = catid
    # filterParamList = [*getFilter(param) for param in params
    session = HttpRequest(True,'https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list?itemsPerPage=1',headers,{},{},False,cpath,1,10)
    if not onlyid:CreatelastupdateLog(session,adsType)
    producer = AsyncKafkaTopicProducer()
    try:
        filterParamList = getFilter(session,params,producer=producer,onlyid=onlyid)
    finally:
        session.__del__()
    # startCrawling(session,filterParamList,producer=producer)
def main_scraper(payload):
    adtype = payload["real_state_type"]
    if adtype == "Updated/Latest Ads":
        UpdateParuvendu()
    elif payload.get("real_state_type") == "deletedCheck":
        rescrapActiveId()
    else:
        main(adtype)
def getLastUpdates():
    try:
        with open(f'{cpath}/lastUpdate.json','r') as file:
            updates = json.load(file)
    except:
        return {}
    return updates
def GetAdUpdate(session,adurl):
    updates = {}
    headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"
    }
    res = fetch(session,adurl,headers=headers,Json=False)
    print(res,"dfsdfsdfsdfsdfsd")
    # doc = await res.text()
    soup = HTML(html = res.text)
    print(adurl,soup)
    modifiedtime = soup.find("meta[property='og:url']",first=True).attrs.get("content").split("=")[1]
    id = soup.find("div[data-id]",first=True).attrs['data-id'] 
    nowtime  = datetime.now()
    updates = {
        "timestamp":nowtime.timestamp(),
        "lastupdate":modifiedtime,
        "lastadId":id,
        "source":adurl
    }
    return updates

def CreatelastupdateLog(session,typ):
    updates = getLastUpdates()
    if typ == "sale":
        catid = "IVH00000"
    else:
        catid = "ILH00000"
    params.update({
        'sortOn':'dateMiseEnLigne',
        'sortTo':'DESC',
        'catId':catid,
        "filters[_R1]":catid,
        'itemsPerPage':1
        })
    d = fetch(session,url,params)
    try:
        data = d['feed']['row'][0]
        adurl = data['shortURL']
        latupdate = GetAdUpdate(session,adurl=adurl)
        # print(latupdate)
        updates.update({typ:latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>" , e)

    # lastupdate = json.load(open(f'{cpath}/lastUpdate.json','r'))
    print(updates)
    with open(f'{cpath}/lastUpdate.json','w') as file:
        file.write(json.dumps(updates))

def asyncUpdateParuvendu():
    updates = getLastUpdates()
    session = HttpRequest(True,'https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list?itemsPerPage=1',headers,{},{},False,cpath,1)
    # async with aiohttp.ClientSession() as session:
    try:
        if not updates:
            CreatelastupdateLog(session,'rental')
            CreatelastupdateLog(session,'sale')
        updates = getLastUpdates()
        for key,val in updates.items():
            CreatelastupdateLog(session,key)
            print("lastupdates",updates)
            if key == "sale":
                catid = "IVH00000"
            else:
                catid = "ILH00000"
            params.update({
            'sortOn':'dateMiseEnLigne',
            'sortTo':'DESC',
            'catId':catid,
            'itemsPerPage':100,
            'showdetail':1
            })
            updated = False
            p=1
            producer = AsyncKafkaTopicProducer()
            while not updated:
                print(f"cheking page {p}")
                adsres = parstItems(session,params,page=p,producer=producer)
                ads = adsres['feed']['row']
                lastadurl = ads[len(ads)-1]['shortURL']
                webupdates = GetAdUpdate(session,lastadurl)
                # savedata(adsres,producer=producer)
                res = val['lastupdate']>webupdates['lastupdate']
                print(f"{val['lastupdate']}>{webupdates['lastupdate']} ={res} and type {key}")
                if res:
                    updated = True
                p+=1
    finally:
        session.__del__()
def rescrapActiveId():
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    # main("buy",True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
        futures = [excuter.submit(main, i,True) for i in ["buy","sale"]]
        for f in futures:print(f)
    print("complited")
    saveLastCheck(website,nowtime.isoformat())
def UpdateParuvendu():
    asyncUpdateParuvendu()
if __name__=="__main__":
    url = "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"
    asyncio.run(main())

