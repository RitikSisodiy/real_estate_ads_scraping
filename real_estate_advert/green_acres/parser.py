from datetime import datetime


def ParseGreenAcre(data):
    """
    Parses the `data` dictionary and extracts relevant information to create a new dictionary.

    Args:
    - data: A dictionary containing the data to be parsed.

    Returns:
    - sdata: A dictionary containing the extracted information from `data`.
      If an error occurs during parsing, it returns an empty dictionary.
    """
    try:
        now = datetime.now()
        assetsDic = {
            info["type"]: info["value"] for info in data.get("characteristics")
        }
        area = assetsDic.get(0) or assetsDic.get(9) or 0
        area = int(area)
        landarea = assetsDic.get(9) or 0
        sellerInfo = data.get("agencyAccount")
        images = data.get("largePics")
        sdata = {
            "id": data.get("id"),
            "ads_type": "buy",
            "price": data.get("price") or 0,
            "original_price": data.get("originalPrice") or data.get("price") or 0,
            "area": area,
            "city": data.get("location"),  # "Poitiers (86000)"
            "declared_habitable_surface": area,
            "declared_land_surface": landarea,
            "land_surface": landarea,
            "declared_rooms": assetsDic.get(1) or 0,
            "declared_bedrooms": assetsDic.get(2) or 0,
            "rooms": assetsDic.get(1) or 0,
            "bedrooms": assetsDic.get(2) or 0,
            "longitude": data.get("latitude"),
            "latitude": data.get("longitude"),
            "location": f'{data.get("latitude")}, {data.get("longitude")}',
            "title": data.get("title"),
            "description": data.get("summary"),
            "postal_code": data.get("postalCode") or "",
            "agency": sellerInfo.get("accountType") == "agency",
            "agency_name": sellerInfo.get("agencyName"),
            "agency_details": {
                "logo": sellerInfo.get("advertiserDisplayInfo").get(
                    "advertiserLogoUrl"
                ),
                "address": sellerInfo.get("address"),
                "name": sellerInfo.get("agencyName"),
                "rcs": sellerInfo.get("siret"),
                "phone": sellerInfo.get("phone") or "",
                "email": sellerInfo.get("email"),
                "website": sellerInfo.get("url"),
                "city_name": sellerInfo.get("city"),
                "zipcode": sellerInfo.get("postalCode"),
            },
            "available": True,
            "status": True,
            "furnished": any(
                word
                in (data.get("summary").lower() + " ".join(data.get("options") or ""))
                for word in ["furnished", "meublée", "meublé"]
            ),
            "coloc_friendly": False,
            "elevator": any(
                word
                in (data.get("summary").lower() + " ".join(data.get("options") or ""))
                for word in ["elevator", "ascenseur"]
            ),
            "pool": any(
                word
                in (data.get("summary").lower() + " ".join(data.get("options") or ""))
                for word in ["piscine", "piscina"]
            ),
            "balcony": any(
                word
                in (data.get("summary").lower() + " ".join(data.get("options") or ""))
                for word in [
                    "balcony",
                    "balcon",
                ]
            ),
            "floor": any(
                word
                in (data.get("summary").lower() + " ".join(data.get("options") or ""))
                for word in ["estage", "floor", "étage"]
            ),
            "terrace": any(
                word
                in (data.get("summary").lower() + " ".join(data.get("options") or ""))
                for word in ["terrace", "terrasse"]
            ),
            "insee_code": data.get("postalCode") or "",
            "parking": any(
                word
                in (data.get("summary").lower() + " ".join(data.get("options") or ""))
                for word in ["parking", "garage"]
            ),
            "images_url": images,
            "is_new": False,
            "website": "green-acres.fr",
            "property_type": data.get("type"),
            "published_at": getTimeStamp(data.get("creationDate")),
            "created_at": getTimeStamp(data.get("lastAdvertUpdate")),
            "last_modified": getTimeStamp(data.get("lastAdvertUpdate")),
            "visite_virtuelle": data.get("visual3DUrl")
            or f'https://www.youtube.com/watch?v={data.get("videoLink")}'
            if data.get("videoLink")
            else "",
            "others": {
                "assets": [
                    f'{info["formattedText"]}:{info["value"]}'
                    for info in data.get("characteristics")
                ]
                if data.get("characteristics")
                else []
            },
            "url": f"https://www.green-acres.fr/fr/properties/{data.get('id')}.htm",
            "dpe": "N/A",
            "ges": "N/A",
            "last_checked": now.isoformat(),
        }

        return sdata
    except:
        open("greenAcer.json", "w").write(json.dumps(data) + "\n")
        return {}
