import json
import time
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
maxresult = 12500 # we can only get 12500 result by one filter url
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?nbp=0&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&ddlFiltres=nofilter"
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?nbp=0&tt=5&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&ddlFiltres=nofilter"
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt=5&at=1&nbp0=99&pa=FR&lo=&codeINSEE="
# filterurl = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?tt=1&at=1&nbp0=99&pa=FR&lo=&codeINSEE="
def getFilter(params):
    dic,baseurl = params , "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"
    # url = getUrl(baseurl,dic)
    url = baseurl
    totalresult =getTotalResult(dic,baseurl)
    acres = totalresult
    fetchedresult = 0
    iniinterval = [0,1000]
    filterurllist = ''
    finalresult = 0
    maxprice = getMaxPrize(params,baseurl)
    if totalresult>=maxresult:
        while iniinterval[1]<=maxprice:
            dic['filters[P5M0]'],dic['filters[P5M1]'] = iniinterval
            totalresult = getTotalResult(dic,baseurl)
            if totalresult < maxresult and maxresult-totalresult<=2000:
                print("condition is stisfy going to next interval")
                print(iniinterval,">apending")
                # filterurllist.append(iniinterval)
                filterurllist+=json.dumps(dic)+":\n"
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
        filterurllist+=json.dumps(dic)
        finalresult +=totalresult
    finallsit = [json.loads(d) for d in filterurllist.split(":\n")]
    print(finallsit)
    # paramslist = []
    # for par in finallsit:
    #     params['filters[P5M0]'],params['filters[P5M1]'] = par
    #     paramslist.append(params)
    print(f"total result is : {acres} filtered result is: {finalresult}")
    time.sleep(10)
    return finallsit
if __name__=="__main__":
    params = {
    'ver':'4.1.4',
    'itemsPerPage':'12',
    'mobId':'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd',
    'p':'1',
    'catId':'IVH00000',
    'filters[_R1]':'IVH00000',
    'sortOn':'dateMiseEnLigne',
    'sortTo':'DESC',
    'key':'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh',
    }
    print(getFilter(params),"++++++++++++++")