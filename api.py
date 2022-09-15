from real_estate_advert.leboncoin.scraperv4 import CheckId as LebonCoinAdStatus
from real_estate_advert.logicImmo.logicImmo import CheckId as LogicImmoAdStatus
from real_estate_advert.paruvendu.scraperv2 import CheckId as ParuvenduAdStatus
from real_estate_advert.pap.scraper2 import CheckId as PapAdStatus
from real_estate_advert.seloger.scraperv3 import CheckId as SelogerAdStatus
from real_estate_advert.bienci.scraper import CheckId as BienciAdStatus
from real_estate_advert.models import RealStateParameter, VendorType, RealStateType, PropertyType,RealStateAdId,RealPortals
from typing import Optional
from fastapi import APIRouter
import tasks
from fastapi import status

"""---------------------------------------- Router Object  for API endpoint ---------------------------------------- """

router = APIRouter()

"""------------------------------------------API Endpoints ------------------------------------------------------- """


@router.post("/real-estate/", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def real_estate(real_args: RealStateParameter, real_state_type: RealStateType,
                      vendor: Optional[VendorType] = None,
                      property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """
    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.real_estate_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.real_estate_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}

    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}


@router.post("/real-estate/leboncoin", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_leboncoin(real_args: RealStateParameter, real_state_type: RealStateType,
                           vendor: Optional[VendorType] = None,
                           property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """
    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrape_leboncoin_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.scrape_leboncoin_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}


@router.post("/real-estate/pap", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_pap(real_args: RealStateParameter, real_state_type: RealStateType, vendor: Optional[VendorType] = None,
                     property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """
    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrape_pap_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.scrape_pap_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}


@router.post("/real-estate/paruvendu", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_paruvendu(real_args: RealStateParameter, real_state_type: RealStateType,
                           vendor: Optional[VendorType] = None,
                           property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """
    payload = dict(real_args)
    print("this is payload )________((((((((",real_state_type)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrape_paruvendu_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.scrape_paruvendu_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}


@router.post("/real-estate/seloger", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_seloger(real_args: RealStateParameter, real_state_type: RealStateType,
                         vendor: Optional[VendorType] = None,
                         property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """

    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrape_seloger_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.scrape_seloger_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}
@router.post("/real-estate/bienci", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_bienci(real_args: RealStateParameter, real_state_type: RealStateType,
                         vendor: Optional[VendorType] = None,
                         property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """

    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrap_bienci_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.scrape_seloger_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}

@router.post("/real-estate/logicimmo", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_logicimmo(real_args: RealStateParameter, real_state_type: RealStateType,
                         vendor: Optional[VendorType] = None,
                         property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """

    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrap_logicimmo_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.scrape_seloger_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}
@router.post("/real-estate/lefigaro", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_lefigaro(real_args: RealStateParameter, real_state_type: RealStateType,
                         vendor: Optional[VendorType] = None,
                         property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """

    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrap_lefigaro_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except tasks.scrape_seloger_task.OperationalError as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Operational Error": str(e)}
        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}

@router.post("/real-estate/avendrealouer", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_avendrealouer(real_args: RealStateParameter, real_state_type: RealStateType,
                         vendor: Optional[VendorType] = None,
                         property_type: Optional[PropertyType] = None):
    """
    :param real_args:
    :param vendor:
    :param real_state_type:
    :param property_type:
    :return:
    """

    payload = dict(real_args)
    if real_state_type:
        try:
            payload.update({"real_state_type": real_state_type})
            if vendor:
                payload.update({"vendor": vendor})
            if property_type:
                payload.update({"property_type": property_type})

            task_id = tasks.scrap_avendrealouer_task.apply_async(kwargs={"payload": payload}, retry=False)
            return {"message": "scraping request is successfully added to web scrapping server",
                    "status": status.HTTP_200_OK,
                    "request_id": str(task_id)}

        except Exception as e:
            return {"message": "scraping request is failed",
                    "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
    else:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "real_state_type is required..."}


@router.post("/real-estate/checkAdStatus", tags=["Real estate Advert"], status_code=status.HTTP_200_OK)
async def scrape_checkAdStatus(real_args: RealStateAdId, Portals:RealPortals):
    payload = dict(real_args)
    if not payload.get("id"):
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "Ad id is required..."}
    if not Portals:
        return {"message": "scraping request is failed",
                "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "exception": "Please Select Ads Portal"}
    payload.update({"Portal": Portals})
    id = payload.get("id")
    if payload['Portal']=="Leboncoin":
        AdStatus = await LebonCoinAdStatus(id) 
    if payload["Portal"] == "Logicimmo":
        AdStatus = LogicImmoAdStatus(id) 
    if payload['Portal'] == "Paruvendu":
        AdStatus = await ParuvenduAdStatus(id)
    if payload['Portal'] == "Seloger":
        AdStatus = SelogerAdStatus(id)
    if payload['Portal'] == "Bienci":
        AdStatus = await BienciAdStatus(id)
    if payload['Portal'] == "Pap":
        AdStatus =  PapAdStatus(id)
    if AdStatus:return{"status":200,"found":True}
    else:return {"status":410,"found":False}