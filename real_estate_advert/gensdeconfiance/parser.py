from datetime import datetime
import re
def ParseGensdeconfiance(data):
    try:
        now = datetime.now()
        adtype= data.get("category").split("__")
        attrs = data.get("attributes") or {}
        realEstateCompany = data.get("realEstateCompany") or {}
        sellerinfo = data.get("owner") or {}
        if (attrs.get("propertyType")=="home"):
            protype = "maison"
        elif (attrs.get("propertyType")=="apartment"):
            protype = "appartement"
        else:
            protype = attrs.get("propertyType")
        if len(adtype)==2:
            adtype = adtype[1]
        else:adtype= adtype[0]
        terrain_pattern = "terrain\s*(\d+.\d+|\d+)\s*m"
        terrain = re.findall(terrain_pattern,data.get('description')) or re.findall(terrain_pattern,data.get('title'))
        terrain = (terrain and terrain[0].replace(".","")) or 0
        sdata = {
            "id": data.get("uuid"),
            "ads_type": "rent" if adtype=="rent" else "buy",
            "price": data.get("price") or 0,
            "original_price": data.get("price") or 0,
            "area": attrs.get("nbSquareMeters"),
            "city": data.get("city"),     # "Poitiers (86000)"
            "declared_habitable_surface": attrs.get("nbSquareMeters") ,
            "declared_land_surface": attrs.get("nbSquareMeters") ,
            "land_surface": attrs.get("nbSquareMeters"),
            "terrain" : terrain,
            "declared_rooms": attrs.get("nbPieces")  or 0,
            "declared_bedrooms":  attrs.get("nbRooms") or 0,
            "rooms": attrs.get("nbPieces")  or 0,
            "bedrooms":attrs.get("nbRooms") or 0,
            "longitude": data.get("longitude"),
            "latitude": data.get("latitude"),
            "location": f'{data.get("latitude")}, {data.get("longitude")}',
            "title": data.get("title"),
            "description": data.get('description'),
            "postal_code": data.get("zip") or "",
            "agency": data.get("owner_type") == "pro",
            "agency_name": realEstateCompany.get("name") or sellerinfo.get("fullName"),
            "agency_details": {
                "logo": realEstateCompany.get("logoUrl") or sellerinfo.get("picture") or sellerinfo.get("face"),
                "name":realEstateCompany.get("name") or sellerinfo.get("fullName"),
                },
            "available": True,
            "status": True,
            "furnished": any(word in (data.get("description").lower()) for word in ["furnished","meublée","meublé"]),
            "last_checked": now.isoformat(),
            "coloc_friendly": False,
            "elevator": any(word in (data.get("description").lower()) for word in ["elevator","ascenseur"]),
            "pool": any(word in (data.get("description").lower()) for word in ["piscine","piscina"]),
            "balcony": any(word in (data.get("description").lower()) for word in ["balcony","balcon",]),
            "floor": any(word in (data.get("description").lower()) for word in ["estage","floor","étage"]),
            "terrace": any(word in (data.get("description").lower()) for word in ["terrace","terrasse"]),
            "insee_code": data.get("postalCode") or "",
            "parking": any(word in (data.get("description").lower()) for word in ["parking","garage"]),
            "images_url": data.get("photos"),
            "is_new": False,
            "website": "gensdeconfiance.com",
            "property_type": protype,
            "published_at": data.get("displayDate"),
            "created_at": data.get("displayDate"),
            "last_modified": data.get("displayDate"),
            "others":{
                "dpe": attrs.get("dpe"),
                "ges": attrs.get("ges"),
            },
            "url": data.get("url"),
            "dpe": attrs.get("dpe"),
            "ges": attrs.get("ges")
        }
        
        return sdata
    except:
        return sdata