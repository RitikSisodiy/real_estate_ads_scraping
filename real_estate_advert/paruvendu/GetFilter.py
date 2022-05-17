# nbp=0 #pieces
# npb0 = 10 #starting point
# npb1 = 10 #ending point point
mpage= 500  #maximum pages we can  
from requests_html import HTMLSession
finalfilterurl = []
roomFilter = []
chamberfilter = []
s = HTMLSession()
def fetch(url):
    # print(url)
    r= s.get(url)
    print(r)
    return r
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

def Inifilter(url):
    r= fetch(url)
    global roomFilter,chamberfilter
    roomFilters = r.html.find("#habitation-liste-nb-pieces input[typeChk=fils]") 
    chamberfilters = r.html.find("#enrNbChb option") 
    for room in roomFilters:
        roomFilter.append(room.attrs.get('value'))
    for cham in chamberfilters:
        val = cham.attrs.get('value')
        if val:
            chamberfilter.append(val)
    return r
    

def CheckFilter(url,ini=False):
    lasturl = url+f"&p={mpage}"
    if ini:
        r =  Inifilter(lasturl)
    else:
        r = fetch(lasturl)
    global finalfilterurl
    # print(lasturl)
    furl = r.url
    #check we have {mpage} pages or not
    dic,baseurl = GetUrlFilterdDict(furl)
    if dic.get('p')==str(mpage):
        return True
    else:
        finalfilterurl.append(r.url)
        return False
def getFilter(url):
    global finalfilterurl,roomFilter
    if CheckFilter(url,ini=True):
        # adding for loop in Room to narrow the query
        for room in roomFilter:
            dic,baseurl = GetUrlFilterdDict(url)
            dic["nbp0"] = room
            dic["nbp1"] = room
            rurl = getUrl(baseurl,dic)
            if CheckFilter(rurl):
                for cham in chamberfilter:
                    dic['enrNbChb'] = cham
                    print(dic)
                    nurl = getUrl(baseurl,dic)
                    print(nurl)
                    # input("chake")
                    if CheckFilter(nurl):
                        print("there is more data than 500 pages",nurl)
                        finalfilterurl.append(nurl)
                        # input("doyou wnat to continue")
    # print(finalfilterurl)
    return finalfilterurl
# url = "https://www.paruvendu.fr/immobilier/annonceimmofo/liste/listeAnnonces?nbp=0&tt=1&tbApp=1&tbDup=1&tbChb=1&tbLof=1&tbAtl=1&tbPla=1&tbMai=1&tbVil=1&tbCha=1&tbPro=1&tbHot=1&tbMou=1&tbFer=1&at=1&nbp0=99&pa=FR&ddlFiltres=nofilter"
# getFilter(url)