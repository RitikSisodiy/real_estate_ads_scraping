from datetime import datetime
from xml.dom import ValidationErr
import requests ,json,re
from .temp import tempDb
db = tempDb()
def try_or(func, default=None, expected_exc=(Exception,)):
    try:
        return func()
    except expected_exc:
        return default
def getSimiFromDic(dic,simName):
    keylist = list(dic.keys())
    for key in keylist:
        if simName in key:
            return dic[key]
    return 
def getTimeStamp(strtime):
    formate = '%Y-%m-%d %H:%M:%S'
    # "2022-09-26 12:00:00"
    t = datetime.strptime(strtime,formate)
    return t.timestamp()
session = requests.session()
def getAjency(id,id2):

    reqUrl = f"https://ws-web.avendrealouer.fr/common/accounts/?id={id}"

    headersList = {
    "Authorization": "Basic ZWQ5NjUwYTM6Y2MwZDE4NTRmZmE5MzYyODE2NjQ1MmQyMjU4ZWMxNjI=",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 8.1.0; Android SDK built for x86 Build/OSM1.180201.007)",
    "Host": "ws-web.avendrealouer.fr",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip" 
    }

    payload = ""

    response = session.get(reqUrl, data=payload,  headers=headersList)
    if response.status_code==200:
        return response.json()
    else:
        print(id," ",id2)
        print(response.status_code)
        input()
        return {}
def getOrCreateDb(id,id2):
    data = db.get(id)
    if not data:
        data = getAjency(id,id2)
        print(data)
        db.create(id,json.dumps(data))
    else:
        try:
            data = json.loads(data[1])
        except:
            print(data)
            raise ValidationErr
    return data
def ParseAvendrealouer(data):
    now = datetime.now()
    # pinRegx = r'(\d{5}\-?\d{0,4})'\
    try:
        location = data.get("viewData")
        cordinates = data.get("location")
        seller = getOrCreateDb(data.get("accountId"),data.get("id"))
        medias = data.get("medias")
        images  = [photo.get("url") for photo in medias.get("photos")] if medias else []
        sellerlogo = seller.get("pro").get("logoUrl") if seller.get("pro") else ""
        sellerweb = seller.get("pro").get("webSiteUrl") if seller.get("pro") else ""
        city_name = seller.get("pro").get("city_name") if seller.get("pro") else ""
        urld = data.get("realms")
        if urld:
            url = data.get("aval").get("url") if data.get("aval") else ""
            if "http" not in url:
                url = "https://www.avendrealouer.fr" + url
    # try
        sdata = {
            "id":"aven"+str(data.get("id")),
            "ads_type": "rent" if data.get("transactionId")==2 else "buy",
            "price": data.get('price'),
            "original_price": data.get("price"),
            "area": data.get("surface"),
            "city": location.get("seoLocalityName"),
            "declared_habitable_surface": data.get("livingSurface") or 0,
            "declared_land_surface": data.get("landSurface") or 0,
            "land_surface": data.get("landSurface"),
            "declared_rooms": data.get("roomsCount"),
            "declared_bedrooms": data.get("bedroomsCount"),
            "rooms": data.get("roomsCount"),
            "bedrooms": data.get("bedroomsCount"),
            "title": data.get("title"),
            "description": data.get("description"),
            "postal_code":  re.search("\(([0-9]+)\)",location.get("localityName")).group(1) if location.get("localityName") else '',
            "longitude": cordinates.get("lon"),
            "latitude": cordinates.get("lat"),
            "location": f"{cordinates.get('lat')}, {cordinates.get('lon')}",
            "agency": True if seller.get("type")=="pro" else False,
            "agency_name": seller.get('name'),
            "agency_details": {
                "logo":sellerlogo,
                "address": seller.get('address'),
                "name": seller.get("name"),
                "rcs": seller.get('siret'),
                "phone": seller.get('telephone'),
                "email": seller.get('email'),
                "website": sellerweb,
                "city_name": city_name,
            },
            "available": True,
            "status": True,
            "furnished": any(word in data.get("description").lower() for word in ["furnished","meublée","meublé"]),
            "last_checked_at": data.get("@timestamp"),
            "coloc_friendly": False,
            "elevator": any(word in data.get("description").lower() for word in ["elevator","ascenseur"]) ,
            "pool":  any(word in data.get("description").lower() for word in ["piscine","piscina"]),
            "balcony": data.get("balconiesCount") or any(word in data.get("description").lower() for word in ["balcon","balcony"]),
            "terrace": data.get("terracesCount") or any(word in data.get("description").lower() for word in ["terrace","terrasse"]),
            "insee_code": location.get("zipcode"),
            "parking": any(word in data.get("description").lower() for word in ["parking","garage"]),
            "images_url": images,
            "is_new": True,
            "website": "avendrealouer.fr",
            "property_type": data.get("title").split(" ")[0] if data.get("title").split(" ") else "",
            "published_at": getTimeStamp(data.get("releaseDate")),
            "created_at": getTimeStamp(data.get("insertDate")),
            "updated_at": getTimeStamp(data.get("insertDate")),
            "others": {
            "assets":[],
            "ges":data.get("diagnostics").get("gasSymbol"),        
            "dpe":data.get("diagnostics").get("energySymbol"),        
            },
            "url": url,
            "ges":data.get("diagnostics").get("gasSymbol"),
            "dpe":data.get("diagnostics").get("energySymbol"),  
            "variation": {
                "price": 0,
                "timestamp": ""
            },
            "last_checked": now.isoformat(),
            "priceDeviation": []
        }
        return sdata
    except:
    #     traceback.print_exc()
    #     with open("error.json","w") as file:
    #         file.write(json.dumps(data))
    #     raise ValidationErr      
    # open("ex.json",'a').write(json.dumps(sdata)+"\n")
        return {}