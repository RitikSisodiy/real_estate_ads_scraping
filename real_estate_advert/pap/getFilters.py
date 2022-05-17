from requests_html import HTMLSession
from unidecode import unidecode
s = HTMLSession()


# type = 'vente' #for sales
type = 'locations' #for rentals
def fetch(url):
    return s.get(url)
# def getFilter(type="vente"):
baseurl = 'https://www.pap.fr/annonce/'
url = f"{baseurl}{type}"
finalfilterurl = []
r = s.get(baseurl)
filterform = r.html.find("form",first=True)
typefilter = [opt.attrs.get("value") for opt in filterform.find("#typesbien option")]
bedroomfilter = [unidecode(opt.text.strip().lower().replace(" ",'-')) for opt in filterform.find("#nb_chambres option")][1:]
bedroomfilter[len(bedroomfilter)-1] = "a-partir-de-"+bedroomfilter[len(bedroomfilter)-1]
roomfilter = [unidecode(opt.text.strip().lower().replace(" ",'-').replace("-et-+",'')) for opt in filterform.find("#nb_pieces option")]

def CheckFilter(url):
    global finalfilterurl
    lasturl = url+"-23"
    print(lasturl)
    r= fetch(lasturl)
    furl = r.url
    #check we have 23 pages or not
    if furl[len(furl)-2:]!="23":
        finalfilterurl.append(r.url)
        return True
    else:
        return False
#funtion to get filter list 
def getFilter(adtype):
    if adtype=='sales' or adtype=="rental":
        if adtype=='sales':adtype='vente'
        if adtype=='rental':adtype='locations'
    else:
        print('invalid ads type')
        return False
    global finalfilterurl
    for typ in typefilter:
        finalfilterurl
        iniurl= f"{baseurl}{adtype} {typ}".replace(" ",'-')
        print(iniurl)
        # break
            #check we have 23 pages or not
        if CheckFilter(iniurl):
            pass
        #there is more than 23 page so make our filter spotlight more small
        else:
            print("trying roomfilter",roomfilter)
            for room in roomfilter:
                roominiurl = f"{iniurl}-{room}"
                if CheckFilter(roominiurl):
                    pass
                else:
                    for broom in bedroomfilter:
                        biniurl = f"{roominiurl}-{broom}"
                        if CheckFilter(biniurl):
                            pass
                        else:
                            finalfilterurl.append(biniurl)
                            print("we have more results you have to make your spotlight more small",biniurl)
    data = finalfilterurl
    finalfilterurl = []
    return data