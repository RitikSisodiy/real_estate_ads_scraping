from datetime import datetime
import re,json
def getTimeStamp(strtime,formate=None):
  try:
    if not formate:
      # 2022-09-05T10:21:18
      formate = '%Y-%m-%dT%H:%M:%S'
    t = datetime.strptime(strtime,formate)
    return t.timestamp()
  except:return None
def ParseLefigaro(data):
  now = datetime.now()
  try:
    adtyp = "sale" if data.get("transactionType")=="vente" else "rent"
    prize  = data.get("priceLabel")
    prize = int(re.search("[0-9.]+",prize).group())
    location = data.get("location")
    rooms =  int(re.search("[0-9]+",data.get("roomCountLabel")).group()) if re.search("[0-9]+",data.get("roomCountLabel")) else 0
    bedrooms =  data.get("bedRoomCount")
    sellerdetail = data.get("client")
    sellerLocation = sellerdetail.get("location")
    area = data.get("area") or 0
    title = f"{data.get('transactionType')} {data.get('type')} "
    if rooms:title+=str(rooms)+ "pièces "
    if area:title += str(area)+ " m²"
    visual = data.get("images")
    images = []
    for imgob in data.get("images").get("photos"):
      img = imgob.get("url")
      keys = list(img.keys())
      keys.sort()
      for key in keys:
        if img[key]:
          images.append(img[key])
    sdata = {
      "id": "lefi-"+data.get("id"),
      "ads_type": adtyp,
      "price": prize,
      "original_price": prize,
      "area": int(area),
      "city": location.get("city"),     # "Poitiers (86000)"
      "declared_habitable_surface": data.get("area"),
      "declared_land_surface": data.get("area"),
      "land_surface": data.get("area"),
      "declared_rooms": rooms,
      "declared_bedrooms": bedrooms,
      "rooms": rooms,
      "bedrooms": bedrooms,
      "longitude": location.get("longitude"),
      "latitude": location.get("latitude"),
      "location": f"{location.get('latitude')}, {location.get('longitude')}",
      "title": title,
      "description": data.get('description'),
      "postal_code": location.get("postalCode") or "",
      "agency": data.get("origin")=="professionnel",
      "agency_name": sellerdetail.get("name"),
      "agency_details": {
        "logo": sellerdetail.get("logoUrl"),
        "address":" ".join([sellerLocation.get("address"),sellerLocation["city"],sellerLocation['postalCode'],sellerLocation["country"]]),
        "name":sellerdetail.get("name"),
        "rcs":sellerdetail.get("siret"),
        "phone":sellerdetail.get("phoneNumber").replace(" ","") if sellerdetail.get("phoneNumber") else "" ,
        "email":sellerdetail.get("email"),
        "website":sellerdetail.get("url"),
        "city_name":sellerLocation.get("city"),
        "zipcode":sellerLocation.get("postalCode"),
        },
      "available": True,
      "status": True,
      "furnished": any(word in (data.get("description").lower() + " ".join(data.get("options") or "")) for word in ["furnished","meublée","meublé"]),
      "last_checked_at": data.get("@timestamp"),
      "coloc_friendly": False,
      "elevator": any(word in (data.get("description").lower() + " ".join(data.get("options") or "")) for word in ["elevator","ascenseur"]),
      "pool": any(word in (data.get("description").lower() + " ".join(data.get("options") or "")) for word in ["piscine","piscina"]),
      "balcony": any(word in (data.get("description").lower() + " ".join(data.get("options") or "")) for word in ["balcony","balcon",]),
      "floor": any(word in (data.get("description").lower() + " ".join(data.get("options") or "")) for word in ["estage","floor","étage"]),
      "terrace": any(word in (data.get("description").lower() + " ".join(data.get("options") or "")) for word in ["terrace","terrasse"]),
      "insee_code": location.get("postalCode"),
      "parking": any(word in (data.get("description").lower() + " ".join(data.get("options") or "")) for word in ["parking","garage"]),
      "images_url": images,
      "is_new": False,
      "website": "immobilier.lefigaro.fr",
      "property_type": data.get("type"),
      "published_at": getTimeStamp(data.get("creationDate")),
      "created_at": getTimeStamp(data.get("updatedAt")),
      "visite_virtuelle":visual.get("3dVisualizationUrl") or visual.get("plan3dUrl") or visual.get("virtualTourUrl"),
      "others":{
        "assets":[opt for opt in data.get("options")] if  data.get("options") else []
      },
      "url": data.get("recordLink"),
      "dpe": data.get("dpe").get("energyconsumptioncategory") or "",
      "ges": data.get("dpe").get("gesemissioncategory") or "",
      "variation": {
                "price": 0,
                "timestamp": ""
            },
            "last_checked": now.isoformat(),
            "priceDeviation": []
      }
    return sdata
  except:
    open("lefigaroerror.json",'w').write(json.dumps(data)+"\n")
    return {}
    traceback.print_exc()
    raise ValidationErr
