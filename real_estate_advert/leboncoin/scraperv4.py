from base64 import encode
from urllib import response
import urllib.request
from datetime import datetime
import json
from datetime import datetime
import time
import os
import gzip
import requests
try:
    from uploader import AsyncKafkaTopicProducer
except:
    from .uploader import AsyncKafkaTopicProducer
cpath =os.path.dirname(__file__)
producer = AsyncKafkaTopicProducer()
def now_time_int():
    dateobj = datetime.now()
    total = int(dateobj.strftime('%S'))
    total += int(dateobj.strftime('%M')) * 60
    total += int(dateobj.strftime('%H')) * 60 * 60
    total += (int(dateobj.strftime('%j')) - 1) * 60 * 60 * 24
    total += (int(dateobj.strftime('%Y')) - 1970) * 60 * 60 * 24 * 365
    return total
def saveAds(res):
    ads = res.get('ads')
    # producer.PushDataList('leboncoin-data_v1',ads)
    totalads = ''
    for ad in ads:
        totalads+= json.dumps(ad)+"\n"
    if totalads:
        with open('out12.json','a') as file:
            file.write(totalads)
class LeboncoinScraper:
    def __init__(self,parameter,outputfilename) -> None:
        try:
            self.cookies  = {}
            with open("cookies.txt",'r') as file:
                cookie = file.read()
                rescookies = cookie.split(";")
                for cookie in rescookies:
                    try:
                        key,val = cookie.split("=")
                        self.cookies[key] = val
                    except:pass
        except:
            cookie = ""
        self.headers = {
            "X-LBC-CC": "7",
            "Accept": "application/json,application/hal+json",
            "User-Agent":"LBC;Android;11;sdk_gphone_x86;phone;8b1263fac1529be6;wifi;5.70.2;570200;0",
            "Content-Type":"application/json; charset=UTF-8",
            "Content-Length":"433",
            "Host": "api.leboncoin.fr",
            "Connection": "Keep-Alive",
            "Accept-Encoding":"gzip",
        }
        if not self.cookies:
            self.cookies = {
                "datadome":'''__Secure-InstanceId=29ded759-24da-4a95-bd60-db96dbe34f46; ry_ry-l3b0nco_realytics=eyJpZCI6InJ5X0M2QzFEQjIzLTExOEQtNDU1Qi1BRTgyLTExMkMwMjFGQUJERSIsImNpZCI6bnVsbCwiZXhwIjoxNjgyOTM2ODMyMTAxLCJjcyI6bnVsbH0=; didomi_token=eyJ1c2VyX2lkIjoiMTgwN2YyOGUtZmIzMS02MWI1LWFjYmEtZTgzYjdhMGU2NjViIiwiY3JlYXRlZCI6IjIwMjItMDUtMDFUMTA6Mjc6MTMuNzI0WiIsInVwZGF0ZWQiOiIyMDIyLTA1LTAxVDEwOjI3OjEzLjcyNFoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzpsYmNmcmFuY2UiLCJjOnJldmxpZnRlci1jUnBNbnA1eCIsImM6ZGlkb21pIl19LCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbInBlcnNvbm5hbGlzYXRpb25tYXJrZXRpbmciLCJwcml4IiwibWVzdXJlYXVkaWVuY2UiLCJleHBlcmllbmNldXRpbGlzYXRldXIiXX0sInZlbmRvcnNfbGkiOnsiZW5hYmxlZCI6WyJnb29nbGUiXX0sInZlcnNpb24iOjIsImFjIjoiRExXQThBRUlBSXdBV1FCLWdHRkFQeUFra0JKWUVBd0lrZ1NrQXR5QnhBRHB3SFZnUlVBam5CSk9DV3NGQ0lLTFFWemdzRkJiZUM0d0Z5d01CZ1lSQXhOQmxxQUEuRExXQTZBRUlBSXdBX1FEQ2dINUFTU0Frc0NBWUVTUUpTQVc1QTRnQjA0RHF3SXFBUnpna25CTFdDaEVGRm9LNXdXQ2d0dkJjWUM1WUdBd01JZ1ltZ3kxQSJ9; euconsent-v2=CPYSgcAPYSgcAAHABBENCMCgAPLAAHLAAAAAIAtB_G_dTyPi-f59YvtwYQ1P4VQnoyACjgaNgwwJiRLBMI0EgmAIKAHqAAACIBAEICJAAQBlCAHAAAAA4IEAASMMAAAAIBAIIgCAAEAAAmJICABZC5AAAQAQgkwAABQAgAICABMgSDAAAAAAFAAAAAgAAAAAAAAAAAAAQAAAAAAAAggCACYalxAA2JY4EkgaRAgARhAEAUAIAKKAIWCCAgJEAAgjAAUQAgAAAoAAAAAACAgBgAAAAEACEAAAADggEABAAgAAAAgAAgAAAAAQAAAYAAAAAABAAAAAAEABAAABQCAAAIAEABIEAAQAAAEAAAAAAAAAEAgAAAAAAAAAAAAAAACAGKAAwABBFgYABgACCLBAADAAEEWA.flgADlgAAAAA; _gcl_au=1.1.1982167932.1651400834; __gads=ID=cefe456db3055e6e:T=1651420637:S=ALNI_MZ9oNOM3vxXCvXKfYnk-jzhxS3pwQ; trc_cookie_storage=taboola%20global%3Auser-id=009a58ff-6ae9-489e-b24e-7dc7ab1a6b4e-tuct967d856; _hjSessionUser_2783207=eyJpZCI6IjNiNTFhYzgzLWQ3NmUtNTk3Ny05YTJhLTRiOWUxNGY3MmUyMiIsImNyZWF0ZWQiOjE2NTE0MDA4MzA1NDAsImV4aXN0aW5nIjp0cnVlfQ==; cto_bundle=OaOwNF9BNzNETWppNW1NczFmREs1eGNxNWZmOVRQQ1AxYUExbG5vS0tMbHBlWGxTenUlMkZHZm0wTWRJRUMyNVZvZkdlZUFVUlk3TWJvQVVTcEFZUSUyRjJqTjBMNlh2MFdkREw3JTJCQXhFcDlZNjlEM0R6ZExsWUV5QVZnakxVTWtqUmE3MjNpY2V4TkxSQlo2d2lqbHZRb21iWDhraUElM0QlM0Q; _pin_unauth=dWlkPVpERXhNbU00T0dFdFltTTFNQzAwWmpjd0xUZ3pPRFF0TmpBeFltTmtPVFF6TVRNdw; __gsas=ID=c6ae0d9301969e37:T=1652240022:S=ALNI_MaklqGvkMlDZE_oXmrs6DYUOtMcng; atidvisitor={"name":"atidvisitor","val":{"an":"NaN","ac":"","vrn":"-562498-"},"options":{"path":"/","session":34128000,"end":34128000}}; atauthority={"name":"atauthority","val":{"authority_name":"default","visitor_mode":"optin"},"options":{"end":"2023-07-11T06:54:20.418Z","path":"/"}}; utag_main=v_id:01807f28edc90021aae8dc69350c05073003d06b00978$_sn:20$_ss:0$_st:1654932260438$_pn:2;exp-session$ses_id:1654930458990;exp-session; __gpi=UID=0000051de22954b6:T=1651420637:RT=1654930463:S=ALNI_Maw2SB_W3CqrcGi9ra6gFaqbRFsgg; datadome=ri2_UGmLPYxVo~ukd3Mh3iyMKOaWW_V3SAZWvptS2WGJR8D4gAIbnRwgZ3vGj_vO2y-Tlz4mwB-6lxXCJtTo6xI~PTYXm8-DNQRAmxBpnwn~N_CE.fSW3v.~4LMY87E; include_in_experiment=true; _pbjs_userid_consent_data=1562905373006922; dblockS=1; dblockV=14datadome=.4klEEX07G7EIZ.Jgqjrr5c.2D2qUO.0C9cQie9oJX-wpD1COJDSAOtVoH.oeGe56xev9qEA0zVWxCROM6nI9xkyp5GyTSRDq1wqYfx~MdU7DGlju9uahi11fxxWVsD-; Max-Age=31536000; Domain=.leboncoin.fr; Path=/; Secure; SameSite=Lax'''
            }
        self.session = requests.session()
        self.parameter = parameter
        self.updateCookies()
        self.autoSave = True
        self.outputfile = outputfilename
        self.searchurl = "https://api.leboncoin.fr/api/adfinder/v1/search"
    def updateCookies(self):
        self.headers['Cookie'] = ";".join([(f"{key}={val}") for key,val in self.cookies.items()])
        with open("cookies.txt",'w') as file:
            file.write(self.headers['Cookie'])
    def CrawlLeboncoin(self):
        parameter= self.parameter
        # headers = {
        #     # cookie=datadome= 
        #     'Cookie': 'didomi_token=eyJ1c2VyX2lkIjoiMmIyN2ZmNDMtODU3NS00MTY4LWFhNzktNDczYjFiMDc4Yjg2IiwidmVuZG9ycyI6eyJlbmFibGVkIjpbImM6cm9ja3lvdSIsImM6cHVib2NlYW4tYjZCSk10c2UiLCJjOnJ0YXJnZXQtR2VmTVZ5aUMiLCJjOnNjaGlic3RlZC1NUVBYYXF5aCIsImM6Z3JlZW5ob3VzZS1RS2JHQmtzNCIsImM6cmVhbHplaXRnLWI2S0NreHlWIiwiYzpsZW1vbWVkaWEtemJZaHAyUWMiLCJjOnlvcm1lZGlhcy1xbkJXaFF5UyIsImM6bWF5dHJpY3NnLUFTMzVZYW05IiwiYzpzYW5vbWEiLCJjOnJhZHZlcnRpcy1TSnBhMjVIOCIsImM6cXdlcnRpemUtemRuZ0UyaHgiLCJjOmxiY2ZyYW5jZSIsImM6cmV2bGlmdGVyLWNScE1ucDV4IiwiYzpyZXNlYXJjaC1ub3ciLCJjOndoZW5ldmVybS04Vllod2IyUCIsImM6YWRtb3Rpb24iLCJjOnRoaXJkcHJlc2UtU3NLd21IVksiLCJjOmFmZmlsaW5ldCIsImM6aW50b3dvd2luLXFhenQ1dEdpIiwiYzpkaWRvbWkiLCJjOmpxdWVyeSIsImM6YWthbWFpIiwiYzphYi10YXN0eSIsImM6emFub3giLCJjOm1vYmlmeSIsImM6YXQtaW50ZXJuZXQiLCJjOnB1cnBvc2VsYS0zdzRaZktLRCIsImM6aW5mZWN0aW91cy1tZWRpYSIsImM6bWF4Y2RuLWlVTXROcWNMIiwiYzpjbG91ZGZsYXJlIiwiYzppbnRpbWF0ZS1tZXJnZXIiLCJjOmFkdmFuc2UtSDZxYmF4blEiLCJjOnJldGFyZ2V0ZXItYmVhY29uIiwiYzp0dXJibyIsImM6Y2FibGF0b2xpLW5SbVZhd3AyIiwiYzp2aWFudC00N3gyWWhmNyIsImM6dnVibGUtY01DSlZ4NGUiLCJjOmJyYW5jaC1WMmRFQlJ4SiIsImM6c2ZyLU1kcGk3a2ZOIiwiYzphcHBzZmx5ZXItWXJQZEdGNjMiLCJjOmhhc29mZmVyLThZeU1UdFhpIiwiYzpsa3FkLWNVOVFtQjZXIiwiYzpzd2F2ZW4tTFlCcmltQVoiLCJjOmZvcnR2aXNpb24taWU2YlhUdzkiLCJjOmFkaW1vLVBoVVZtNkZFIiwiYzpvc2Nhcm9jb20tRlJjaE5kbkgiLCJjOnJldGVuY3ktQ0xlclppR0wiLCJjOmlsbHVtYXRlYy1DaHRFQjRlayIsImM6YWRsaWdodG5pLXRXWkdyZWhUIiwiYzpyb2NrZXJib3gtZlRNOEVKOVAiXSwiZGlzYWJsZWQiOltdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJjb29raWVzIiwic2VsZWN0X2Jhc2ljX2FkcyIsImNyZWF0ZV9hZHNfcHJvZmlsZSIsIm1lYXN1cmVfYWRfcGVyZm9ybWFuY2UiLCJtYXJrZXRfcmVzZWFyY2giLCJpbXByb3ZlX3Byb2R1Y3RzIiwic2VsZWN0X3BlcnNvbmFsaXplZF9hZHMiLCJnZW9sb2NhdGlvbl9kYXRhIiwicGVyc29ubmFsaXNhdGlvbm1hcmtldGluZyIsInByaXgiLCJtZXN1cmVhdWRpZW5jZSIsImV4cGVyaWVuY2V1dGlsaXNhdGV1ciJdLCJkaXNhYmxlZCI6W119fQ==;__Secure-InstanceId=b4ad7045-7ac5-4fab-a719-b0e745767de5;',                   
        #     'X-LBC-CC': '7',
        #     'Accept': 'application/json,application/hal+json',
        #     'User-Agent': 'LBC;Android;7.1.2;ASUS_Z01QD;phone;a3ab60b8d97f0f1a;wifi;5.69.0;569000;0',
        #     'Content-Type': 'application/json; charset=utf-8',
        #     'Host': 'api.leboncoin.fr',
        #     'Connection': 'Keep-Alive',
        #     # 'Accept-Encoding': 'gzip',
        # }
        
        jsondata = json.dumps(parameter)
        jsondataasbytes = jsondata.encode()  
        print(self.headers)
        req = urllib.request.Request("https://api.leboncoin.fr/api/adfinder/v1/search",headers=self.headers)
        req.add_header('Content-Length', len(jsondataasbytes))
        response = urllib.request.urlopen(req, jsondataasbytes)
        # response = opener.open(req, jsondataasbytes)
        # response = requests.post(self.searchurl, headers=self.headers, verify=False,json=parameter)
        time.sleep(5)
        # print(response.status_code)
        # if response.status_code==200:
        print(response.getcode())
        if response.getcode()==200:
            resheaders = dict(response.headers._headers)
            # resheaders = response.headers
            rescookies = resheaders.get('set-cookie')
            if rescookies:
                rescookies = rescookies.split(";")
                for cookie in rescookies:
                    try:
                        key,val = cookie.split("=")
                        self.cookies[key] = val
                    except:pass
                self.updateCookies()
            # data = response.read()
            data = gzip.decompress(response.read())
            # encoding = response.info().get_content_charset()
            # print(encoding)
            # data =data.decode("ISO-8859–1")
            # print(data)
            res= json.loads(data)
            if self.autoSave:
                self.saveAds(res)

            # res= response.json()
            return res
        else:
            print(response.status_code,"some issue do you wanna retry y/n")
            # ch = input()
            # if ch=="Y" or ch=="y":
            time.sleep(300)
            return self.CrawlLeboncoin(parameter)
            # else:
            #     return False
    def checkNext(self,res):
        try:
            next = res['pivot']
            self.parameter['pivot'] = next
            return next
        except:
            return False

    def IntCrawling(self):
        self.autoSave = True
        res = self.CrawlLeboncoin()
        nextpage = self.checkNext(res)
        while nextpage:
            print(nextpage)
            self.parameter['pivot'] = nextpage
            res = self.CrawlLeboncoin()
            nextpage = self.checkNext(res)
    def saveAds(self,res):
        ads = res.get('ads')
        if not ads:
            return 0
        producer.PushDataList('leboncoin-data_v2',ads)
        # totalads = ''
        # for ad in ads:
        #     totalads+= json.dumps(ad)+"\n"
        # if totalads:
        #     with open(f'{cpath}/{self.outputfile}.json','a') as file:
        #         file.write(totalads)
    def UpdataAds(self):
        self.autoSave = False
        try:
            with open(f"{cpath}/lastUpdate.json","r") as file:
                lastUpdate = json.load(file)
            lastTime = self.getTimeStamp(lastUpdate['addUploadTime'])
            updated = False
        except Exception as e:
            print(e)
            print("please check your lastUpdate.json file")
            raise e 
        first = True
        while not updated:
            res = {'ads':[]}
            resdata  = self.CrawlLeboncoin()
            # print(resdata)
            # break
            for data in resdata["ads"]:
                if first:
                    nowtime  = datetime.now()
                    nowtime = nowtime.timestamp()
                    lastupdate = {
                        "time":nowtime,
                        "source":data['url'],
                        "adid":data['list_id'],
                        "addUploadTime":data['index_date']
                    }
                    first = False
                    with open(f"{cpath}/lastUpdate.json",'w') as file:
                        file.write(json.dumps(lastupdate))
                if lastTime<self.getTimeStamp(data["index_date"]):
                    res['ads'].append(data)
                    # print("appending")
                else:
                    print("very old add",data)
                    updated = True
                    break
            self.saveAds(res)
            nextpage = self.checkNext(resdata)
            if not nextpage:
                print("no pages left")
                break
            

    def getTimeStamp(self,strtime):
        formate = '%Y-%m-%d %H:%M:%S'
        t = datetime.strptime(strtime,formate)
        return t.timestamp()
def updateLebonCoin():
    data = json.load(open(f'{cpath}/filter.json','r'))
    ob = LeboncoinScraper(data,"newout1")
    # ob.IntCrawling()
    ob.UpdataAds()
def leboncoinAdScraper(payload):
    typ = payload.get("real_state_type")
    if typ == "Updated/Latest Ads":
        updateLebonCoin()
    else:
        data = json.load(open(f'{cpath}/filter.json','r'))
        ob = LeboncoinScraper(data,"newout1")
        ob.IntCrawling()
if __name__=="__main__":
    data = json.load(open(f'{cpath}/filter.json','r'))
    ob = LeboncoinScraper(data,"newout1")
    # ob.IntCrawling()
    ob.UpdataAds()    
