import re
from datetime import datetime
from unidecode import unidecode
def GetTImeStamp(time):
  d,m,y = time.split(" ")
  dic2 = {'janvier': 'January', 'fevrier': 'February', 'mars': 'March', 'avril': 'April', 'mai': 'May', 'juin': 'June', 'juillet': 'July', 'aout': 'August', 'septembre': 'September', 'octobre': 'October', 'novembre': 'November', 'decembre': 'December'}
  m = dic2[unidecode(m).lower()][:3].lower()
  date = f"{d}/{m}/{y}"
  formate = "%d/%m/%Y"
  t = datetime.strptime(date,formate)
  return int(t.timestamp())
def GetTImeStamp(time):
  d,m,y = time.split(" ")
  dic2 = {'janvier': 'January', 'fevrier': 'February', 'mars': 'March', 'avril': 'April', 'mai': 'May', 'juin': 'June', 'juillet': 'July', 'aout': 'August', 'septembre': 'September', 'octobre': 'October', 'novembre': 'November', 'decembre': 'December'}
  m = dic2[unidecode(m).lower()][:3].lower()
  date = f"{d}/{m}/{y}"
  formate = "%d/%b/%Y"
  t = datetime.strptime(date,formate)
  return int(t.timestamp())
# with open("pap_data_v1_response.json",'r') as file:
#     data = json.load(file)
def ParsePap(data):
  # data = data["_source"]
  now = datetime.now()
  try:
    adtype = data.get("produit")
    if adtype=="location":adtype="rent"
    else:adtype = "buy"
    cityRe = "^[a-zA-ZÀ-ÿ-. ]*"
    caracteristiquesstr = unidecode(data.get("caracteristiques")).split(" / ")[1:]
    caracteristiquesdic = {key.split(" ")[1]:key.split(" ")[0] for key in caracteristiquesstr} 
    try:
      pin = re.search(r"(\()([0-9]+)(\))",data.get("titre")).group(2)
    except: pin=""
    title = f'{data.get("typebien")} {data.get("typebien") } {caracteristiquesdic.get("mA2")} {data.get("titre")}'.replace("  "," ").replace("None","")
    for key in caracteristiquesstr:
      key = key.split(" ")
      if "Terrain" in key:
        caracteristiquesdic[key[0]] = key[1]
      else:
        caracteristiquesdic[key[1]] = key[0]
    try:price = re.search(r"[0-9]+",unidecode(data.get("prix"))).group()
    except:price = 0
    area = caracteristiquesdic.get("mA2") or caracteristiquesdic.get("m2") or ""
    if area: area = str(area).replace(",",'.')
    sdata = {
      "id":data.get("id"),
      "ads_type": adtype,
      "price": float(price),
      "original_price": float(price),
      "area":  area,
      "city": re.search(cityRe,data.get("titre")).group(),
      "declared_habitable_surface":  area,
      "declared_land_surface": caracteristiquesdic.get("Terrain"),
      "land_surface": caracteristiquesdic.get("Terrain"),
      "declared_rooms":data.get("nb_pieces"),
      "declared_bedrooms": data.get("nb_chambres_max"),
      "rooms": data.get("nb_pieces"),
      "bedrooms": data.get("nb_chambres_max"),
      "title": title,
      "description": data.get("texte"),
      "postal_code": pin,
      "latitude": data.get("marker").get("lat"),
      "longitude": data.get("marker").get("lng"),
      "location": f"{data.get('marker').get('lat')}, {data.get('marker').get('lng')}",
      "agency": False,
      "agency_name": None,
      "agency_details": {},
      "available": True,
      "status": True,
      "furnished": any(word in data.get("texte").lower() for word in ["furnished","meublée","meublé"]),
      "coloc_friendly": False,
      "elevator": any(word in data.get("texte").lower() for word in ["elevator","ascenseur"]),
      "pool": any(word in data.get("texte").lower() for word in ["piscine","piscina"]),
      "balcony": any(word in data.get("texte").lower() for word in ["balcony","balcon"]),
      "floor": 0,
      "terrace": any(word in data.get("texte").lower() for word in ["terrace","terrasse"]),
      "insee_code": pin,
      "parking": any(word in data.get("texte").lower() for word in ["parking","stationnement"]),
      "images_url": data.get("photos"),
      "is_new": False,
      "website": "pap.fr",
      "property_type": data.get("typebien"),
      "published_at": None,
      "created_at": GetTImeStamp(data.get("date")),
      "last_modified": GetTImeStamp(data.get("date")),
      "visite_virtuelle": data.get("visite_virtuelle"),
      "url": data.get("url"),
      "dpe": data.get("classe_dpe").get("letter") if data.get("classe_dpe") else None,
      "ges": data.get("classe_ges").get("letter") if data.get("classe_ges") else None,
            "last_checked": now.isoformat(),
    }
    return sdata
  except: 
    return {}
