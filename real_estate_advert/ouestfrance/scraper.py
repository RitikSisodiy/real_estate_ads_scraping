from operator import imod
import os,traceback,requests,time,json
from datetime import datetime,timedelta
from tqdm import tqdm

from saveLastChaeck import saveLastCheck
from .parser import ParseOuestfrance
from HttpRequest.uploader import AsyncKafkaTopicProducer
cpath =os.path.dirname(__file__) or "."
kafkaTopicName = "ouestfrance-immo-v1"
commanPattern ="common-ads-data_v1"
website= "ouestfrance-immo.com"
commonIdUpdate = f"activeid-{website}"
class OuestFranceScraper:
    def __init__(self,paremeter,timeout = 5) -> None:
        self.logfile = open(f"{cpath}/error.log",'a')
        self.timeout = timeout
        self.headers = {
            "Host": "api-phalcon.ouestfrance-immo.com",
            "Connection": "keep-alive",
            "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
            "Accept": "application/json, text/plain, */*",
            "sec-ch-ua-mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            "sec-ch-ua-platform": "Windows",
            "Origin": "https://www.ouestfrance-immo.com",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://www.ouestfrance-immo.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        }
        self.paremeter = paremeter
        self.size=100
        self.producer = AsyncKafkaTopicProducer()
        self.searchurl = "https://www-api.ouestfrance-immo.com/api/annonces/"
        self.session = requests.Session()
    def init_session(self):
        self.session.close()
        self.session = requests.Session()
    def fetch(self,url,method = "get",sid=0,retry=0,**kwargs):
        kwargs['headers'] = self.headers
        try:
            if method=="post":
                r = self.session.post(url,timeout=self.timeout,**kwargs)
            else:
                r = self.session.get(url,timeout=self.timeout,**kwargs)
            # print(f"{r.status_code} : response status")
        except Exception as e:
            traceback.print_exc(file=self.logfile)
            print(e)
            time.sleep(1)
            self.init_session()
            if retry<10:
                retry+=1
                return self.fetch(url,method=method,sid=sid,retry=retry,**kwargs)
            else:return None
        if r.status_code not in [200,404,400]:
            print(url)
            print(method)
            print(kwargs)
            self.init_session(sid=sid)
            if retry<10:
                retry+=1
                return self.fetch(url,method=method,sid=sid,retry=retry,**kwargs)
            else:return None
        return r
    def save(self,adslist,onlyid=False):
        if onlyid:
            now = datetime.now()
            ads = [{"id":"quest-"+str(ad.get("id")), "last_checked": now.isoformat(),"available":True} for ad in adslist]
            self.producer.PushDataList_v1(commonIdUpdate,ads)
        else:
            self.producer.PushDataList(kafkaTopicName,adslist)
            adslist = [ParseOuestfrance(ad) for ad in adslist]
            self.producer.PushDataList(commanPattern,adslist)
        # data = ""
        # for ad in adslist:
        #     data += json.dumps(ad)+"\n"
        # with open(f"{cpath}/out.json","a") as file:
        #     file.write(data)
        # parseAdList = [ParseLogicImmo(ad) for ad in adslist]
        # self.producer.PushDataList(commonTopicName,parseAdList)
        # self.producer.PushDataList(nortifyTopic,parseAdList)
    def Crawlparam(self,param,allPage = True,first=False,save=True,onlyid=False):
        # input()
        response = self.fetch(self.searchurl, method = "get", params=param,)
        if not response:
            return 0
        # print(response.status_code,"+++++++++")
        res = response.json()
        pagesize = param["limit"]
        ads = res['data']
        fetchedads = ads
        if save:self.save(fetchedads,onlyid=onlyid)
        if first:
            return fetchedads[0]
        if allPage:
            totalpage = res["count"]
            # input()
            print(len(ads),"_total ads+++++++++++++")
            totalpage = int(totalpage/pagesize)+1
            print(f"total page {totalpage}")
            for i in tqdm(range(2,totalpage)):
                param["page"] = i
                self.Crawlparam(param,allPage=False,onlyid=onlyid)
                # break
        else:
            return fetchedads
    def getTotalResult(self,dic):
        dic = {
            **dic,
            "limit":1
        }
        response = self.fetch(self.searchurl, method = "get", params=dic)
        res = response.json()
        count = res["count"]
        return count
    def getMax(self,dic,max="prix"):
        dic = {
            **dic,
            "tri":f"{max}_desc",
            f"{max}_min":1,
            "limit":1
        }
        response = self.fetch(self.searchurl, method = "get", params=dic)
        res = response.json()
        maxval = res["data"][0][max]
        return maxval
    def genFilter(self,parameter={},onlyid=False,low="prix_min",max="prix_max"):
        if parameter:
            dic = parameter.copy()
        else:parameter = self.paremeter.copy()
        # totalresult = getTotalResult(session,dic)
        # acres = totalresult
        # dic['recherche[produit]']=typ
        if low=="prix_min":
            # iniinterval =[98976, 98989]
            iniinterval = [0,1000]
            maxprize = self.getMax(dic.copy())
        else:
            maxprize = self.getMax(dic.copy(),max="surface")
            iniinterval =[0,1]
        # maxprize = getMaxPrize(session,dic)
        maxresult = 4999
        filterurllist = ""
        finalresult = 0
        nooffilter = 0
        while iniinterval[1]<=maxprize:
            dic[low],dic[max] = iniinterval
            totalresult = self.getTotalResult(dic)
            if totalresult <= maxresult and totalresult>0:
                # print("condition is stisfy going to next interval",totalresult)
                # print(iniinterval,">apending")
                filterurllist += json.dumps(iniinterval) + "/n/:"
                # FetchFilter(dic,onlyid,session2)
                self.CrawlOuestfrance(dic.copy(),onlyid)
                print("going to next")
                iniinterval[0] = iniinterval[1]+1
                iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
                finalresult +=totalresult
                nooffilter +=1
            # elif acres-finalresult<4800 and acres-finalresult>0:
            #     print(maxresult, acres , maxresult-acres)
            #     # input()
            #     # print("elif 1")
            #     last = 10
            #     iniinterval[1] = maxprize
            elif totalresult == 0:
                # print("elif 1",iniinterval)
                last = 10
                iniinterval[0] = iniinterval[1]
                iniinterval[1] = iniinterval[0] + (int(iniinterval[1]/last) or 1)
                # iniinterval[0] = iniinterval[1] 
                # iniinterval[1] +=1
            elif iniinterval[1]-iniinterval[0] <=2 and totalresult>maxresult and low=="prix_min":
                finalresult +=totalresult
                self.genFilter(dic.copy(),onlyid,"surface_min","surface_max")
                iniinterval[0] = iniinterval[1] 
                iniinterval[1]+=1 
            elif totalresult>maxresult:
                # print("elif 2",iniinterval)
                last = -5
                dif = iniinterval[1]-iniinterval[0]
                iniinterval[1] = iniinterval[1] + int(dif/-2) 
                if iniinterval[0]>iniinterval[1]:
                    iniinterval[1] = iniinterval[0]+1
            # retrydic[iniinterval[0]] +=1
            print(f"\r{totalresult}-{maxresult}:::: of  {finalresult}==>{iniinterval} {maxprize} {low} no of filter",end="")
            # print(totalresult,"-",maxresult,"::::",acres ," of ", finalresult,"==>",iniinterval)
        self.CrawlOuestfrance(dic.copy(),onlyid)

    def getLastUpdate(self):
        try:
            with open(f"{cpath}/lastUpdate.json",'r') as file:
                data = json.load(file)
            return data
        except:
            return {}
    def getTimeStamp(self,strtime):
        formate = '%Y-%m-%d %H:%M:%S'
        # 2022-06-19 05:26:55
        try:
            t = datetime.strptime(strtime,formate)
        except:return strtime
        return t.timestamp()
    def getUpdateDicFromAd(self,ad):
        nowtime  = datetime.now()
        upd={
                "timestamp": nowtime.timestamp(),
                "lastupdate": self.getTimeStamp(ad.get('date_deb_aff')),
                "Rlastupdate": ad.get('date_deb_aff'),
                "created": self.getTimeStamp(ad.get('date_creation')),
                "rcreated": ad.get('date_creation'),
            }
        return upd
    def getAdStatus(self,id):
        url = f"{self.searchurl}{id}"
        r = self.fetch(url)
        data = r.json()
        if data["data"]:return True
        else:False
    def getlatestAd(self):
        param = self.paremeter
        param["limit"] = 1
        data = self.Crawlparam(param,allPage=False,first=True)
        param["limit"] = self.size
        return data
    def createNewUpdate(self,latestad=None):
        lastupd = self.getLastUpdate()
        if not latestad:
            latestad = self.getlatestAd()
        lastupd=self.getUpdateDicFromAd(latestad)
        with open(f"{cpath}/lastUpdate.json",'w') as file:
            file.write(json.dumps(lastupd))
    def CrawlOuestfrance(self,param=None,onlyid=False):
        if not param:
            param = self.paremeter.copy()
        if not onlyid:self.createNewUpdate(latestad=None)
        self.Crawlparam(param,onlyid=onlyid)
    def updateLatestAd(self):
        updates = self.getLastUpdate()
        if updates:
            param = self.paremeter
            updated = False
            first = True
            page =1
            adcount = 0
            page = 1
            while not updated:
                param["page"] = page 
                # res = self.fetch(searchurl, method = "post", json=param)
                ads = self.Crawlparam(param,False,False,False)
                updatedads = []
                updatetimestamp = updates["lastupdate"]
                for ad in ads:
                    # ad = self.GetAdInfo(ad['id'])
                    adtimestamp = self.getTimeStamp(ad["date_deb_aff"])
                    print(f"   {adtimestamp}> {updatetimestamp}====>",adtimestamp>updatetimestamp)
                    if adtimestamp>updatetimestamp:
                        if first:
                            self.createNewUpdate(ad)
                            first=False
                        updatedads.append(ad)
                        adcount+=1
                    else:
                        print(f"{adcount} new ads scraped ")
                        updated = True
                        break
                self.save(updatedads)
                page+=1
        else:
            print("there is no update available")
def CheckId(id):
    ob = OuestFranceScraper(data)
    found= ob.getAdStatus(id)
    ob.__del__()
    return found
def main_scraper(payload,update=False):
    adtype = payload.get("real_state_type")
    """
    typIds
        201 : for vente
        214 : for location
        216 : for location
    """
    data = {
        "limit":200,
        "tri":"date_desc",
        # "typIds":"201,214,216"
    }
    if adtype == "Updated/Latest Ads" or update:
        ob = OuestFranceScraper(data,timeout=10)
        print("updateing latedst ads")
        ob.updateLatestAd()
    elif payload.get("real_state_type") == "deletedCheck":
        rescrapActiveId()
    else:
        ob = OuestFranceScraper(data,timeout=30)
        ob.genFilter()
def rescrapActiveIdbyType():
    """
    typIds
        201 : for vente
        207 : for vente
        208 : for vente
        209 : for vente
        210 : for vente
        213 : for location
        214 : for location
        216 : for location
    """
    data = {
        "limit":208,
        "tri":"date_desc",
        # "typIds":"201,214,216"
    }
    ob = OuestFranceScraper(data,timeout=30)
    ob.genFilter(data,onlyid=True)
def rescrapActiveId():
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    rescrapActiveIdbyType()
    # main("buy",True)
    # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
    #     futures = [excuter.submit(rescrapActiveIdbyType, i) for i in ["rental","sale"]]
    #     for f in futures:print(f)
    # rescrapActiveIdbyType("rental")
    # print("complited")
    saveLastCheck(website,nowtime.isoformat())
if __name__ == "__main__":
    """
    typIds
        201 : for vente
        214 : for location
        216 : for location
    """
    data = {
        "limit":200,
        "tri":"date_desc",
        # "typIds":"201,214,216"
    }
    ob = OuestFranceScraper(data,timeout=30)
    ob.CrawlOuestfrance()
    # ob.updateLatestAd()

#parms ?limit=2&tri=date_decroissant&balcon=1&criteres_or=1&hasPhoto=1&idslieu=33540&isNeuf=0&limit=10&terrasse=1&tra=V&typIds=201&veranda=1&prix_min=199290&page=1&prix_max=200000