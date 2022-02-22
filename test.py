from main import app
import os
import sys
from typing import Optional
from fastapi.testclient import TestClient
from real_estate_advert.models import RealStateParameter, VendorType, RealStateType, PropertyType

# Declaring test client
client = TestClient(app)


'''---------------------------------- END Point Testing --------------------------------------'''


def test_real_estate():
    """ asserting status code """
    response = client.post("/api/v1/real-estate/?real_state_type=sale", json={
        "text": "string",
        "min_price": 0,
        "max_price": 0,
        "city": "string",
        "rooms": 0
    })
    assert response.status_code == 200
    assert response.json() == {"message": "scraping request is successfully added to web scrapping server",
                               "status": 200}


def test_scrape_leboncoin():
    """ asserting status code """
    response = client.post("/api/v1/real-estate/?real_state_type=sale", json={
        "text": "string",
        "min_price": 0,
        "max_price": 0,
        "city": "string",
        "rooms": 0
    })
    assert response.status_code == 200
    assert response.json() == {"message": "scraping request is successfully added to web scrapping server",
                               "status": 200}


def test_scrape_pap():
    """ asserting status code """
    response = client.post("/api/v1/real-estate/?real_state_type=sale", json={
        "text": "string",
        "min_price": 0,
        "max_price": 0,
        "city": "string",
        "rooms": 0
    })
    assert response.status_code == 200
    assert response.json() == {"message": "scraping request is successfully added to web scrapping server",
                               "status": 200}


def test_scrape_paruvendu():
    """ asserting status code """
    response = client.post("/api/v1/real-estate/?real_state_type=sale", json={
        "text": "string",
        "min_price": 0,
        "max_price": 0,
        "city": "string",
        "rooms": 0
    })
    assert response.status_code == 200
    assert response.json() == {"message": "scraping request is successfully added to web scrapping server",
                               "status": 200}


def test_scrape_seloger():
    """ asserting status code """
    response = client.post("/api/v1/real-estate/?real_state_type=sale", json={
        "text": "string",
        "min_price": 0,
        "max_price": 0,
        "city": "string",
        "rooms": 0
    })
    assert response.status_code == 200
    assert response.json() == {"message": "scraping request is successfully added to web scrapping server",
                               "status": 200}
