import json,re
from datetime import datetime
def getTimeStamp(strtime,formate=None):
  try:
    if not formate:
      formate = '%Y-%m-%d %H:%M:%S'
    t = datetime.strptime(strtime,formate)
    return int(t.timestamp())
  except:return 0
def ParseParuvendu(data):
  now = datetime.now()
  try:
    adtyp = data.get("catLabel")
    detaillist = data.get("syndication")["detail"]['feed']['row']
    detailT = {}
    detailL = {}
    detailV = {}
    assetlist = []
    for value in detaillist:
        id = value["id"]
        detailL[id] = value.get("title")
        detailT[id] = value.get("text")
        detailV[id] = value.get("value")
        assetlist.append(f"{value['title']}:{value.get('text')}")
    sellerdetail = data.get("syndication")["seller"]["feed"]["row"]
    spec = data.get("syndication")["description"]["feed"]["row"]
    images = [img.get("img") for img in data.get("syndication")["pics"]["feed"]["row"]]
    if "Location" in adtyp:adtyp = "rent"
    else:adtyp="buy"
    area = data.get("title")[data.get("title").rfind("- ")+2:].replace("m²","").strip()
    try:area = int(area)
    except: area = detailV.get("7770503_90")
    roomscount = detailT.get("NBP")
    bedroomscount = detailT.get("9999999_10")
    if roomscount and "+" in roomscount:roomscount = roomscount.replace("+","").strip()
    if roomscount and "/" in roomscount:roomscount = roomscount[roomscount.find("/")+1:]
    if bedroomscount and "+" in bedroomscount:bedroomscount = bedroomscount.replace("+","").strip()
    if bedroomscount and "/" in bedroomscount:bedroomscount = bedroomscount[bedroomscount.find("/")+1:]
    
    pinRegx = r'(\d{5}\-?\d{0,4})'
    ges = detailT.get("GES") 
    if ges:ges = ges if "-" not in ges else ges.split("-",1)[0].strip()
    dpe = detailT.get("DPE")
    if dpe:dpe = dpe if "-" not in dpe else dpe.split("-",1)[0].strip()
    sdata = {
      "id": data.get("id"),
      "ads_type": adtyp,
      "price": int(data.get("price").replace(" ",'')),
      "original_price": int(data.get("price").replace(" ",'')),
      "area": area ,
      "city": data.get("subtitle")[:data.get("subtitle").find(" (")],     # "Poitiers (86000)"
      "declared_habitable_surface": int(detailV.get("SUR")) if detailV.get("SUR") else 0,
      "declared_land_surface": detailT.get("9999999_125"),
      "land_surface": detailT.get("9999999_125"),
      "terrain" : detailT.get("9999999_125"),
      "declared_rooms": roomscount,
      "declared_bedrooms": bedroomscount,
      "rooms": roomscount,
      "bedrooms": bedroomscount,
      "title": data.get("title"),
      "description": spec["text"],
      "postal_code": re.search(pinRegx,spec.get("address")).group() if re.search(pinRegx,spec.get("address")) else "",
      "agency": sellerdetail.get("type")=="PRO",
      "agency_name": sellerdetail.get("company"),
      "agency_details": {
        "address":sellerdetail.get("address"),
        "name":sellerdetail.get("company"),
        "rcs":sellerdetail.get("rcs"),
        "phone":sellerdetail.get("sellerPhone"),
        "email":sellerdetail.get("email"),
        "logo":sellerdetail.get("logo").replace('https://media.paruvendu.fr/media-logo/https://media.paruvendu.fr/media-logo/', 'https://media.paruvendu.fr/media-logo/') if sellerdetail.get("logo") else "",
        "website":sellerdetail.get("website"),
        "city_name":sellerdetail.get("city"),
        "state":sellerdetail.get("state"),
        "zipcode":sellerdetail.get("zipcode"),
        },
      "available": True,
      "status": True,
      "furnished": any(word in spec.get("text").lower() for word in ["furnished","meublée","meublé"]),
      "last_checked_at": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
      "coloc_friendly": False,
      "elevator": any(word in spec.get("text").lower() for word in ["elevator","ascenseur"]),
      "pool": any(word in spec.get("text").lower() for word in ["piscine","piscina"]),
      "balcony": bool(int(detailV.get("_BT"))) if detailV.get("_BT") else False,
      "floor": detailT.get("Etage"),
      "terrace": (bool(int(detailV.get("_JT"))) if detailV.get("_JT") else False) or any(word in spec.get("text").lower() for word in ["terrace","terrasse"]),
      "insee_code": re.search(pinRegx,spec.get("address")).group() if re.search(pinRegx,spec.get("address")) else "",
      "parking": bool(int(detailV.get("_PG"))) if detailV.get("_PG") else False,
      "images_url": images,
      "is_new": bool(int(data.get("new"))) if data.get("new") else False,
      "website": "paruvendu.fr",
      "property_type": detailT.get("_R2").lower(),
      "published_at": data.get("datePublish"),
      "created_at": getTimeStamp(data.get("datePublish"),"%d/%m/%Y"),
      "last_modified": getTimeStamp(data.get("datePublish"),"%d/%m/%Y"),
      "others":{
        "assets":assetlist
      },
      "url": data.get("shortURL"),
      "location":"0,0",
      "last_checked": now.isoformat(),


      "dpe": dpe,
      "ges": ges,
      "estage":detailT.get("9999999_80",0),
      "floorCount": detailT.get("9999999_85",0),
      "bathrooms":detailT.get("9999999_30",0) or detailT.get("7770503_180",0),
      }
    return sdata
  except:
    open("paruvendu.json",'a').write(json.dumps(data)+"\n")
    return {}