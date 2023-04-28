from datetime import datetime
import json,re
import traceback
from xml.dom import ValidationErr
def getTimeStamp(strtime):
        formate = '%Y-%m-%dT%H:%M:%S'
        # 2022-06-19T05:26:55
        t = datetime.strptime(strtime,formate)
        return int(t.timestamp())
def getOrNone(dic,path):
  val = dic
  # try:
  for i in path.split("."):
    try:i=int(i)
    except:pass
    val= val[i]
  return val
  # except:
  #    return None
def ParseSeloger(data):
  now = datetime.now()
  assetlist = []
  if not data:
     return data
  if data.get("features"):
    for d in data.get("features"):
      if d:assetlist.append(d.get("label") )
  isdata = {
    "Balcon": False,
    "Parking": False,
    "Terrasse": False,
    "Ascenseur": False,
    "Piscine": False,
    "Rez-de-chaussée":False
  }
  for lab in assetlist:
    for k,v in isdata.items():
      if k in lab:isdata[k]=True
  professionals = (data.get("professionals") or {}) and data.get("professionals")[0]
  # pinRegx = r'(\d{5}\-?\d{0,4})'
  try:
    sdata = {
        "id":data.get("id"),
        "ads_type": "rent" if data.get("transactionType")==1 else "buy",
        "price": data.get("price"),
        "original_price": data.get("price"),
        "area": data.get("livingArea"),
        "city": data.get("city"),
        "declared_habitable_surface": data.get("livingArea"),
        "declared_land_surface": data.get("livingArea"),
        "land_surface": data.get("livingArea"),
        "declared_rooms": data.get("rooms"),
        "declared_bedrooms": data.get("bedrooms"),
        "rooms": data.get("rooms"),
        "bedrooms": data.get("bedrooms"),
        "title": data.get("title"),
        "description": data.get("description"),
        "postal_code":  data.get("zipCode"),
        "longitude": getOrNone(data,"coordinates.longitude") or "0",
        "latitude": getOrNone(data,"coordinates.latitude") or "0",
        "location": f'{getOrNone(data,"coordinates.latitude") or "0"}, {getOrNone(data,"coordinates.longitude") or "0"}',
        "agency": True if professionals.get('type')==1 else False,
        "agency_name": professionals.get('name'),
        "agency_details": {
          "address": professionals.get('address'),
          "name": professionals.get("name"),
          "rcs": professionals.get('rcs'),
          "phone": professionals.get('phoneNumber') and professionals.get('phoneNumber').replace(" ",""),
          "email": professionals.get('email'),
          "website": professionals.get('website'),
          "url_seloger": professionals.get('url'),
          "logo": professionals.get("logoUrl"),
          "city_name": professionals.get("logoUrl"),
          "id": professionals.get('id')
        },
        "available": True,
        "status": True,
        "last_checked_at": now.isoformat(),
        "coloc_friendly": False,
        "furnished": any(word in (data.get("description") and data.get("description").lower()) for word in ["furnished","meublée","meublé"]),
        "elevator": isdata.get("Ascenseur") or any(word in (data.get("description") and data.get("description").lower()) for word in ["elevator","ascenseur"]),
        "pool": isdata.get("Piscine")  or any(word in (data.get("description") and data.get("description").lower()) for word in ["piscine","piscina"]),
        "floor": isdata.get('Rez-de-chaussée') or any(word in (data.get("description") and data.get("description").lower()) for word in ["estage","floor","étage"]),
        "balcony": isdata.get("Balcon") or any(word in (data.get("description") and data.get("description").lower()) for word in ["balcony","balcon",]),
        "terrace": isdata.get("Terrasse") or any(word in (data.get("description") and data.get("description").lower()) for word in  ["terrace","terrasse"]),
        "insee_code": data.get("zipCode"),
        "parking": isdata.get("Parking") or any(word in (data.get("description") and data.get("description").lower()) for word in  ["parking","garage"]),
        "images_url": data.get("photos"),
        "is_new": True if data.get("isNew") else False,
        "website": "seloger.com",
        "property_type": re.search(r"(annonces/[a-z-]*)/([a-z]*)",data.get("permalink")).group(2),
        "published_at": data.get("created"),
        "created_at": getTimeStamp(data.get("lastModified")),
        "last_modified": getTimeStamp(data.get("lastModified")),
        "others": {
          "assets":assetlist,
          "ges":getOrNone(data,"energyBalance.ges.category"),        
          "dpe":getOrNone(data,"energyBalance.dpe.category"),        
        },
        "url": data.get("permalink"),
        "ges":getOrNone(data,"energyBalance.ges.category"),
        "dpe":getOrNone(data,"energyBalance.dpe.category"),
        "last_checked": now.isoformat(),
      }
  except:
    traceback.print_exc()
    with open("error.json","w") as file:
      file.write(json.dumps(data))
    raise ValidationErr      
  print("parsed")
  return sdata