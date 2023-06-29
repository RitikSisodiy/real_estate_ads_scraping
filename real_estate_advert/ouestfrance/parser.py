from datetime import datetime
from xml.dom import ValidationErr
from temp import tempDb
import json,requests,traceback
session = requests.session()
db = tempDb()
def getTimeStamp(strtime):
        formate = '%Y-%m-%d %H:%M:%S'
        # 2022-06-19T05:26:55
        # "2022-10-28 08:07:14"
        t = datetime.strptime(strtime,formate)
        return int(t.timestamp())
def getFieldLlstStartWith(start,datadic):
  res = {}
  for key,val in datadic.items():
    if start in key:
      if val=="true":val=True
      if val=="false":val=False
      res[key+"s"] = bool(val)
  return res
def getAjency(id):
    reqUrl = f"https://www-api.ouestfrance-immo.com/api/clients/{id}"

    headersList = {
        "Host": "www-api.ouestfrance-immo.com",
        "Connection": "keep-alive",
        "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
        "Accept": "application/json, text/plain, */*",
        "sec-ch-ua-mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        "sec-ch-ua-platform": "Windows",
        "Origin": "https://www.ouestfrance-immo.com",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://www.ouestfrance-immo.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    }
    response = session.get(reqUrl, headers=headersList)
    if response.status_code==200:
        data =response.json()
        if data.get("success"):
          return data["data"]
        else: return {}
    else:
        return {}
def getOrCreateDb(id):
    data = db.get(id)
    if not data:
        data = getAjency(id)
        db.create(id,json.dumps(data))
    else:
        try:
            data = json.loads(data[1])
        except:
            print(data)
            raise ValidationErr
    return data
def ParseOuestfrance(data):
  # data = data["_source"]
  now = datetime.now()
  title = ""
  seller = getOrCreateDb(data.get("cli"))
  location = data.get("lieu") or {}
  if location:postalcode = location.get("cp") or location.get("insee")  
  else:postalcode = ""
  if data.get("lieu_encode"): title+=data.get("lieu_encode")+" "
  if data.get("typ_lib"): title += data.get("typ_lib")+" "
  if data.get("surface"): title += str(data.get("surface"))+ "m²"
  if data.get("nb_pieces"): title += str(data.get("nb_pieces"))+ "pièce"
  try:
    sdata = {
        "id":"quest-"+str(data.get("id")),
        "ads_type": "buy" if data.get("transac_lib_encode")=="vente" else "rent",
        "price": data.get("prix") or 0,
        "original_price": data.get("prix") or 0,
        "area": data.get("surface") or 0,
        "city": data.get("lieu_encode"),
        "declared_habitable_surface": data.get("surface") or 0,
        "declared_land_surface": data.get("surface_terrain") or 0,
        "land_surface": data.get("surface_terrain") or 0,
        "terrain":data.get("surface_terrain") or 0,
        "declared_rooms": data.get("nb_pieces") or 0,
        "declared_bedrooms": data.get("nb_chambre") or 0,
        "rooms": data.get("nb_pieces") or 0,
        "bedrooms": data.get("nb_chambre") or 0,
        "title": title,
        "description": data.get("texte"),
        "postal_code":  postalcode,
        "longitude": location.get('lng'),
        "latitude": location.get('lat'),
        "location": f"{location.get('lat')}, {location.get('lng')}",
        "agency": data.get("pro") == "1",
        "agency_name": seller.get("enseigne"),
        "agency_details": {
          "address": seller.get("adresse"),
          "name":  seller.get("enseigne"),
          "rcs":  seller.get("siret"),
          "phone":  seller.get("vitrine_telephone"),
          "email":  seller.get("vitrine_email"),
          "logo": seller.get("urlLogo") or seller.get("urlPhoto"),
          "zipcode":seller.get("code_postal"),
          "website":seller.get("vitrine_site")
        },
        "available": True,
        "status": True,
        "last_checked": now.isoformat(),
        "coloc_friendly": False,
        "furnished": any(word in (data.get("texte").lower()) for word in ["furnished","meublée","meublé"]),
        "elevator": any(word in data.get("texte").lower() for word in ["elevator","ascenseur"]),
        "pool": any(word in data.get("texte").lower() for word in ["piscine","piscina"]),
        "elevator": bool(data.get("has_ascenseur")) or any(word in (data.get("texte").lower()) for word in ["elevator","ascenseur"]),
        "balcony":bool(data.get("has_balcon")) or any(word in (data.get("texte").lower()) for word in ["balcony","balcon",]),
        "floor": any(word in (data.get("texte").lower()) for word in ["estage","floor","étage"]),
        "terrace": bool(data.get("has_terrasse")) or any(word in (data.get("texte").lower()) for word in ["terrace","terrasse"]),
        "parking": any(word in (data.get("texte").lower()) for word in["parking","garage"]),
        "insee_code": postalcode,
        "images_url": data.get("photos"),
        "is_new": True if data.get("isNew") else False,
        "website": "ouestfrance-immo.com",
        "property_type": data.get("typ_encode"),
        "published_at": getTimeStamp(data.get("date_creation")),
        "created_at": getTimeStamp(data.get("date_deb_aff")),
        "last_modified": getTimeStamp(data.get("date_deb_aff")),
        "visite_virtuelle": (data.get("has_visite_virtuelle") and data.get("url_visite_virtuelle")) or "",
        "url":f"https://www.ouestfrance-immo.com/immobilier/vente/appartement/paris-7e-75-75107/3-pieces-{data.get('id')}.htm",
        "others": {
          "assets":[],
          **getFieldLlstStartWith("has",data),
          "ges":data.get("ges_lettre") or "",
          "dpe":data.get("dpe_lettre") or "",        
        },



        "dpe":data.get("dpe_lettre","NA"),
        "ges":data.get("ges_lettre","NA"),
        "estage":data.get("etage",0),
        "floorCount": data.get("nb_etages",0),
        "bathrooms":data.get("nb_salles_de_bain",0),
        "toilets":data.get("nb_salles_d_eau",0),
        "exposure":data.get("exposition","NA")
      }
  except:
    traceback.print_exc()
    with open("error.json","w") as file:
      file.write(json.dumps(data))
    # raise ValidationErr      
  return sdata
