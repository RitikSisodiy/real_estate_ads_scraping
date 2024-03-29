import json
import traceback
from xml.dom import ValidationErr
from datetime import datetime


def getTimeStamp(strtime):
    formate = "%Y-%m-%d %H:%M:%S"
    t = datetime.strptime(strtime, formate)
    return int(int(t.timestamp()))


def ParseLeboncoin(data):
    """
    Parses the `data` dictionary and extracts relevant information to create a new dictionary.

    Args:
    - data: A dictionary containing the data to be parsed.

    Returns:
    - sdata: A dictionary containing the extracted information from `data`.
      If an error occurs during parsing, it returns an empty dictionary.
    """
    now = datetime.now()
    try:
        featuresli = data.get("attributes")
        features = {}
        featuresVals = {}
        assetlist = []
        featureslabels = {}
        if featuresli:
            for d in featuresli:
                features[d.get("key")] = d.get("value")
                featuresVals[d.get("key")] = d.get("values")
                featureslabels[d.get("key")] = d.get("value_label")
                assetlist.append(f"{d.get('key')} {d.get('value_label')}")
        price = data.get("price_cents") / 100 if data.get("price_cents") else 0
        area = features.get("square")
        try:
            area = float(str(area).strip())
        except:
            area = 0
        location = data.get("location")
        seller = data.get("owner")
        images = data.get("images").get("urls_large")
        sdata = {
            "id": data.get("list_id"),
            "ads_type": "buy" if features.get("lease_type") == "sell" else "rent",
            "price": price,
            "original_price": price,
            "area": area,
            "city": location.get("city"),
            "declared_habitable_surface": features.get("square"),
            "declared_land_surface": features.get("land_plot_surface"),
            "land_surface": features.get("land_plot_surface"),
            "terrain": features.get("land_plot_surface"),
            "declared_rooms": features.get("rooms"),
            "declared_bedrooms": features.get("bedrooms"),
            "rooms": features.get("rooms"),
            "bedrooms": features.get("bedrooms"),
            "title": data.get("subject"),
            "description": data.get("body"),
            "postal_code": location.get("zipcode"),
            "longitude": location.get("lng"),
            "latitude": location.get("lat"),
            "location": f"{location.get('lat')}, {location.get('lng')}",
            "agency": True if seller.get("type") == "pro" else False,
            "agency_name": seller.get("name"),
            "agency_details": {
                "address": seller.get("address"),
                "name": seller.get("name"),
                "rcs": seller.get("siren"),
                "phone": seller.get("phoneNumber"),
                "email": seller.get("email"),
                "website": seller.get("website"),
                "city_name": seller.get("logoUrl"),
                "id": seller.get("store_id"),
            },
            "available": True,
            "status": True,
            "furnished": features.get("furnished")
            or any(
                word in data.get("body").lower()
                for word in ["furnished", "meublée", "meublé"]
            ),
            "last_checked_at": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "coloc_friendly": False,
            "elevator": any(
                word in data.get("body").lower() for word in ["elevator", "ascenseur"]
            ),
            "pool": any(
                word in data.get("body").lower() for word in ["piscine", "piscina"]
            ),
            "floor": features.get("nb_floors_house"),
            "balcony": "balcony" in featuresVals.get("outside_access")
            if featuresVals.get("outside_access")
            else False,
            "terrace": "terrace" in featuresVals.get("outside_access")
            if featuresVals.get("outside_access")
            else False,
            "insee_code": location.get("zipcode"),
            "parking": features.get("nb_parkings"),
            "images_url": images,
            "is_new": bool(features.get("immo_sell_type")),
            "website": "leboncoin.com",
            "property_type": featureslabels.get("real_estate_type"),
            "published_at": getTimeStamp(data.get("first_publication_date")),
            "created_at": getTimeStamp(data.get("index_date")),
            "last_modified": getTimeStamp(data.get("index_date")),
            "others": {
                "assets": assetlist,
                "ges": features.get("ges"),
                "dpe": features.get("energy_rate"),
            },
            "url": data.get("url"),
            "last_checked": now.isoformat(),
            "dpe": features.get("energy_rate", "NA"),
            "ges": features.get("ges", "NA"),
            "estage": data.get("floor_number", 0),
            "floorCount": data.get("nb_floors_building", 0),
        }
    except:
        traceback.print_exc()
        with open("error.json", "w") as file:
            file.write(json.dumps(data))
        raise ValidationErr
    return sdata
