from datetime import datetime
import json,re
import traceback
from xml.dom import ValidationErr
def getFieldLlstStartWith(start,datadic):
  res = {}
  for key,val in datadic.items():
    if start in key:
      res[key] = val
  return res
def ParseLogicImmo(data):
  # data = data["_source"]
  now = datetime.now()
  # pinRegx = r'(\d{5}\-?\d{0,4})'
  title = ""
  if data.get("city"): title+=data.get("city")+" "
  if data.get("propertyType"): title += data.get("propertyType")+" "
  if data.get("area"): title += str(data.get("area"))+ "m²"
  if data.get("rooms"): title += str(data.get("rooms"))+ "pièce"
  energy = data.get("energyBalance")
  if energy:
    ges = energy.get("ges").get("category") or 0
    dpe = energy.get("dpe").get("category") or 0
  else:
    ges,dep = 0,0
  try:
    sdata = {
        "id":data.get("id"),
        "ads_type": "buy" if data.get("transactionTypeId")==1 else "rent",
        "price": data.get("price"),
        "original_price": data.get("price"),
        "area": data.get("area"),
        "city": data.get("city"),
        "declared_habitable_surface": data.get("area"),
        "declared_land_surface": data.get("area"),
        "land_surface": data.get("area"),
        "declared_rooms": data.get("rooms"),
        "declared_bedrooms": data.get("bedrooms"),
        "rooms": data.get("rooms"),
        "bedrooms": data.get("bedrooms"),
        "title": title,
        "description": data.get("description"),
        "postal_code":  data.get("zipCode"),
        # "longitude": data['coordinates']['longitude'],
        # "latitude": data['coordinates']['latitude'],
        # "location": f"{data['coordinates']['latitude']}, {data['coordinates']['longitude']}",
        "agency": bool(data.get("agencyName")) or False,
        "agency_name": data.get("agencyName"),
        "agency_details": {
          "address": data.get("agencyAddress"),
          "name":  data.get("agencyName"),
          "rcs":  data.get("agencySiret"),
          "phone":  data.get("agencyPhone"),
          "email":  data.get("agencyMail"),
          "logo": data.get("agencyLogo"),
          "city_name": data.get("agencyCity"),
          "zipcode":data.get("agencyZipCode")
        },
        "available": True,
        "status": True,
        "last_checked_at": data.get("@timestamp"),
        "coloc_friendly": False,
        "elevator": any(word in data.get("description").lower() for word in ["elevator","ascenseur"]),
        "pool": data.get("hasPool"),
        "floor": data.get("floors"),
        "balcony": data.get("hasBalcony"),
        "terrace": data.get("hasTerrace"),
        "insee_code": data.get("zipCode"),
        "parking": data.get("parkings"),
        "images_url": data.get("photos"),
        "is_new": True if data.get("isNew") else False,
        "website": "logic-immo.com",
        "property_type": data.get("propertyType"),
        "published_at": data.get("firstOnlineDate")-(3600*3),
        "created_at": data.get("updateDate")-(3600*3),
        "others": {
          "assets":[],
          **getFieldLlstStartWith("has",data),
          "ges":ges,        
          "dpe":dpe,        
        },
        "url": data.get("url"),
        "ges":ges,
        "dpe":dpe ,
            "last_checked": now.isoformat(),
      }
  except:
    traceback.print_exc()
    with open("error.json","w") as file:
      file.write(json.dumps(data))
    # raise ValidationErr      
  print("parsed")
  return sdata
