import json,re
from datetime import datetime
def getTimeStamp(strtime,formate=None):
  try:
    if not formate:
      formate = '%Y-%m-%d %H:%M:%S'
    t = datetime.strptime(strtime,formate)
    return t.timestamp()
  except:return None
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
    except: area = int(detailV.get("SUR")) if detailV.get("SUR") else 0

    pinRegx = r'(\d{5}\-?\d{0,4})'
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
      "declared_rooms": detailT.get("NBP"),
      "declared_bedrooms": detailT.get("9999999_10"),
      "rooms": detailT.get("NBP"),
      "bedrooms": detailT.get("9999999_10"),
      "title": data.get("title"),
      "description": spec["text"],
      "postal_code": re.search(pinRegx,spec.get("address")).group() if re.search(pinRegx,spec.get("address")) else "",
      "agency": sellerdetail.get(type)=="PRO",
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
      "updated_at": getTimeStamp(data.get("dateRefresh"),"%d/%m/%Y") or  getTimeStamp(data.get("datePublish"),"%d/%m/%Y"),
      "others":{
        "assets":assetlist
      },
      "url": data.get("shortURL"),
      "dpe": detailT.get("GES"),
      "ges": detailT.get("DPE"),
      "location":"0,0",
      "variation": {
                "price": 0,
                "timestamp": ""
            },
            "last_checked": now.isoformat(),
            "priceDeviation": []
      }
    return sdata
  except:
    open("paruvendu.json",'a').write(json.dumps(data)+"\n")
    return {}