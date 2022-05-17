from real_estate_advert.models import RealStateParameter, VendorType, RealStateType, PropertyType
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
    print("this is payload )________((((((((",payload)
    if real_state_type:
        try:
            # payload.update({"real_state_type": real_state_type})
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
