from datetime import datetime
import json,re
import traceback
from xml.dom import ValidationErr
def getTimeStamp(strtime):
        formate = '%Y-%m-%dT%H:%M:%S'
        # 2022-06-19T05:26:55
        t = datetime.strptime(strtime,formate)
        return int(t.timestamp())
def ParseSeloger(data):
  now = datetime.now()
  assetlist = [d.get("label") for d in data.get("features")]
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
  professionals = data.get("professionals")[0]
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
        "longitude": data['coordinates']['longitude'],
        "latitude": data['coordinates']['latitude'],
        "location": f"{data['coordinates']['latitude']}, {data['coordinates']['longitude']}",
        "agency": True if professionals['type']==1 else False,
        "agency_name": professionals['name'],
        "agency_details": {
          "address": professionals.get('address'),
          "name": professionals.get("name"),
          "rcs": professionals.get('rcs'),
          "phone": professionals.get('phoneNumber').replace(" ",""),
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
        "furnished": any(word in (data.get("description").lower()) for word in ["furnished","meublée","meublé"]),
        "elevator": isdata["Ascenseur"] or any(word in (data.get("description").lower()) for word in ["elevator","ascenseur"]),
        "pool": isdata["Piscine"]  or any(word in (data.get("description").lower()) for word in ["piscine","piscina"]),
        "floor": isdata['Rez-de-chaussée'] or any(word in (data.get("description").lower()) for word in ["estage","floor","étage"]),
        "balcony": isdata["Balcon"] or any(word in (data.get("description").lower()) for word in ["balcony","balcon",]),
        "terrace": isdata["Terrasse"] or any(word in (data.get("description").lower()) for word in  ["terrace","terrasse"]),
        "insee_code": data.get("zipCode"),
        "parking": isdata["Parking"] or any(word in (data.get("description").lower()) for word in  ["parking","garage"]),
        "images_url": data.get("photos"),
        "is_new": True if data.get("isNew") else False,
        "website": "seloger.com",
        "property_type": re.search(r"(annonces/[a-z-]*)/([a-z]*)",data.get("permalink")).group(2),
        "published_at": data.get("created"),
        "created_at": getTimeStamp(data.get("lastModified")),
        "last_modified": getTimeStamp(data.get("lastModified")),
        "others": {
          "assets":assetlist,
          "ges":data.get("energyBalance")["ges"].get("category"),        
          "dpe":data.get("energyBalance")["dpe"].get("category"),        
        },
        "url": data.get("permalink"),
        "ges":data.get("energyBalance")["ges"].get("category"),
        "dpe":data.get("energyBalance")["dpe"].get("category"),
        "last_checked": now.isoformat(),
      }
  except:
    traceback.print_exc()
    with open("error.json","w") as file:
      file.write(json.dumps(data))
    raise ValidationErr      
  print("parsed")
  return sdata