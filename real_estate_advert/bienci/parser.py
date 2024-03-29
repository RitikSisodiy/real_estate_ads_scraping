from datetime import datetime


def getFieldLlstStartWith(start, datadic):
    """
    Retrieves the fields from `datadic` dictionary whose keys start with `start`.

    Args:
    - start: A string representing the starting characters of the keys.
    - datadic: A dictionary containing key-value pairs.

    Returns:
    - res: A dictionary containing key-value pairs from `datadic` where the keys start with `start`.
    """
    res = {}
    for key, val in datadic.items():
        if start in key:
            res[key] = val
    return res


def getTimeStamp(strtime):
    """
    Converts a string timestamp to a Unix timestamp.

    Args:
    - strtime: A string representing the timestamp in the format '%Y-%m-%dT%H:%M:%S.%fZ'.

    Returns:
    - An integer representing the Unix timestamp.
      If the conversion fails, it returns 0.
    """
    try:
        formate = "%Y-%m-%dT%H:%M:%S.%fZ"
        # 1970-01-01T00:00:00.000Z
        t = datetime.strptime(strtime, formate)
        return int(t.timestamp())
    except:
        return 0


def ParseBienici(data):
    """
    Parses the `data` dictionary and extracts relevant information to create a new dictionary.

    Args:
    - data: A dictionary containing the data to be parsed.

    Returns:
    - sdata: A dictionary containing the extracted information from `data`.
      If an error occurs during parsing, it returns an empty dictionary.
    """
    now = datetime.now()
    propername = {"flat": "appartement", "house": "maison", "programme": "maison"}
    # Extract timestamps and sort them in descending order
    dates = [
        getTimeStamp(data.get(date))
        for date in ["publicationDate", "thresholdDate", "modificationDate"]
    ]
    dates.sort(reverse=True)
    try:
        sdata = {
            "id": data.get("id"),
            "ads_type": data.get("adType"),
            "price": data.get("price"),
            "original_price": data.get("price"),
            "area": data.get("surfaceArea"),
            "city": data.get("city"),
            "declared_habitable_surface": data.get("surfaceArea"),
            "declared_land_surface": data.get("landSurfaceArea"),
            "land_surface": data.get("landSurfaceArea"),
            "declared_rooms": data.get("roomsQuantity"),
            "declared_bedrooms": data.get("bedroomsQuantity"),
            "rooms": data.get("roomsQuantity"),
            "bedrooms": data.get("bedroomsQuantity"),
            "title": data.get("title"),
            "description": data.get("description"),
            "postal_code": data.get("postalCode"),
            "longitude": data.get("blurInfo")["position"]["lon"],
            "latitude": data.get("blurInfo")["position"]["lat"],
            "location": f"{data.get('blurInfo')['position']['lat']} ,{data.get('blurInfo')['position']['lon']}",
            "agency": bool(data.get("accountType")),
            "agency_name": None,
            "agency_details": {},
            "available": True,
            "status": True,
            "furnished": any(
                word in data.get("description").lower()
                for word in ["furnished", "meublée", "meublé"]
            ),
            "last_checked_at": data.get("@timestamp"),
            "elevator": data.get("hasElevator")
            or any(
                word in data.get("description").lower()
                for word in ["elevator", "ascenseur"]
            ),
            "pool": bool(data.get("hasPool"))
            or any(
                word in data.get("description").lower()
                for word in ["piscine", "piscina"]
            ),
            "balcony": bool(
                data.get("balconyQuantity") and int(data.get("balconyQuantity"))
            )
            or any(
                word in data.get("description").lower()
                for word in ["balcon", "balcons", "balcony"]
            ),
            "terrace": bool(data.get("terracesQuantity"))
            or any(
                word in data.get("description").lower()
                for word in [
                    "terrace",
                    "terrasse",
                ]
            ),
            "insee_code": data.get("district").get("insee_code"),
            "parking": bool(data.get("parkingPlacesQuantity"))
            or any(
                word in data.get("description").lower()
                for word in [
                    "parking",
                    "garage",
                    "stationnement",
                    "parcage",
                ]
            ),
            "images_url": [photo.get("url") for photo in data.get("photos")],
            "is_new": data.get("newProperty"),
            "website": "bienici.com",
            "property_type": propername.get(data.get("propertyType")),
            "published_at": data.get("publicationDate"),
            "created_at": dates[0],
            "last_modified": dates[0],
            "others": {
                "assets": [],
                **getFieldLlstStartWith("has", data),
            },
            "url": f"https://www.bienici.com/annonce/{data.get('id')}",
            "last_checked": now.isoformat(),
            "terrain": data.get("landSurfaceArea"),
            "estage": data.get("floor", 0),
            "floorCount": data.get("floorQuantity"),
            "bathrooms": data.get("bathroomsQuantity", 0),
            "dpe": data.get("energyClassification", "NA"),
            "ges": data.get("greenhouseGazClassification", "NA"),
        }
        return sdata
    except:
        return {}
