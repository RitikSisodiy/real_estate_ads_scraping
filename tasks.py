import time
from real_estate_advert.leboncoin.scraper import leboncoinAdScraper
from real_estate_advert.leboncoin.scraper import rescrapActiveId as rescrapLeboncoinActiveId
from real_estate_advert.paruvendu.scraper import main_scraper as ParuvenduScraper
from real_estate_advert.paruvendu.scraper import rescrapActiveId as rescrapParuvenduActiveId
from real_estate_advert.gensdeconfiance.scraper import main_scraper as gensdeconfianceScraper
from real_estate_advert.gensdeconfiance.scraper import rescrapActiveId as rescrapGensdeconfianceActiveId
from real_estate_advert.paruvendu.scraper import UpdateParuvendu
from real_estate_advert.pap.scraper import pap_scraper as PapScraper
from real_estate_advert.pap.scraper import UpdatePap
from real_estate_advert.pap.scraper import rescrapActiveId as rescrapPapActiveId
from real_estate_advert.bienci.scraper import main_scraper as bienciScraper
from real_estate_advert.bienci.scraper import UpdateBienci,rescrapActiveId
from real_estate_advert.seloger.scraperv3 import main_scraper as selogerScraper
from real_estate_advert.seloger.scraperv3 import rescrapActiveId as rescrapSelogerActiveId
from real_estate_advert.logicImmo.scraper import main_scraper as LogicImmoScraper
from real_estate_advert.logicImmo.scraper import rescrapActiveId as rescraplogicImmoActiveId
from real_estate_advert.lefigaro.scraper import main_scraper as LefigaroScrapper
from real_estate_advert.lefigaro.scraper import rescrapActiveId as rescrapLefigaroActiveId
from real_estate_advert.ouestfrance.scraper import main_scraper as OuestFranceScrapper
from real_estate_advert.ouestfrance.scraper import rescrapActiveId as rescrapOuestFranceActiveId
from real_estate_advert.avendrealouer.scraper import main_scraper as avendrealouerScrapper
from real_estate_advert.avendrealouer.scraper import rescrapActiveId as rescrapAvendrealouerActiveId
from real_estate_advert.green_acres.scraper import main_scraper as greenacresrScrapper
from real_estate_advert.MissingAdFields.MissingFieldsUpdate import updateMissinField
from celery import Celery
from celery.schedules import crontab
from celery.signals import task_prerun,task_postrun
from celery.result import AsyncResult
from settings import *
from celery.signals import worker_ready
from celery_singleton import clear_locks
from celery_singleton import Singleton
from celery.exceptions import SoftTimeLimitExceeded

worker_concurrency = 20
celery_app = Celery(TaskQueue, backend=CeleryBackend, broker=CeleryBroker)
celery_app.config_from_object(__name__)
runningTasks = set()
# ignore the task if it is already running
@worker_ready.connect
def unlock_all(**kwargs):
    clear_locks(celery_app)
@celery_app.task
def keep_one_active_task():
    while True:
        print("checking dublicate tasks....")
        task_names = set()
        for task in celery_app.tasks.values():
            if task.name and  "." not in task.name:task_names.add(task.name)

        i = celery_app.control.inspect()
        cleaned = []
        for task_name in task_names:
            active_tasks = i.active()
            active_task_ids = [task["id"] for task in [value for key,value in active_tasks.items()][0] if task["name"] == task_name]
            reserved_tasks = i.items()
            reserved_task_ids = [task["id"] for task in  [value for key,value in reserved_tasks.items()][0] if task["name"] == task_name and task["state"]=="PENDING"]
            if len(active_task_ids) > 1:
                for task_id in active_task_ids[1:]:
                    celery_app.control.revoke(task_id, terminate=True)
                cleaned.append(task_name)
            for task_id in reserved_task_ids:
                celery_app.control.revoke(task_id, terminate=True)
                cleaned.append(task_name)
        print(" ".join(cleaned),": are cleaned")
        time.sleep(100)
# keep_one_active_task.apply_async()
# @task_received.connect
# def task_received_handler(request=None,**kwargs):
#     inspect = celery_app.control.inspect()
#     print("inside received")
#     activetasks = inspect.active()#{'celery@DESKTOP-G6UNTK5': [{'id': 'eac73c1f-4368-4db7-8c1f-831cb8c2a13a', 'name': 'real estate test-task', 'args': [], 'kwargs': {'payload': {'text': '', 'min_price': 0.0, 'max_price': 0.0, 'city': '', 'rooms': 0, 'real_state_type': 'Updated/Latest Ads'}}, 'type': 'real estate test-task', 'hostname': 'celery@DESKTOP-G6UNTK5', 'time_start': 1675773176.4143288, 'acknowledged': True, 'delivery_info': {'exchange': '', 'routing_key': 'celery', 'priority': 0, 'redelivered': False}, 'worker_pid': 2471040022664}]}
#     if activetasks:
#         activetasks = [taskinfo["name"] for taskinfo in [value for key,value in activetasks.items()][0] if taskinfo.get("name")]
#         task_name = request.task.name
#         if task_name in activetasks:
#             # results = [AsyncResult(task.id) for task in celery_app.tasks.values() if task.name == task_name]
#             # print(f"ignoring the dublicate task and removing it from waiting queue: {task_name}")
#             # # for result in results:result.revoke()
#             # print(results)
#             result = AsyncResult(request.id)
#             result.revoke()
#             # keep_one_active_task(inspect,task_name)
#             # # print(result)
#             # print(inspect.reserved())
#             return False
runningTasks = set()
# # ignore the task if it is already running
@task_prerun.connect
def task_prerun_handler(task_id, task, args, kwargs, **kw):
    global runningTasks
    if task.name in runningTasks:
        print('Stopping task:', task.name)
        result = AsyncResult(task_id)
        result.revoke()
        return False  # This will stop the task execution
    if task.name:runningTasks.add(task.name)
    print('Starting task:', task.name)
@task_postrun.connect
def task_postrun_handler(task_id, task, args, kwargs, **kw):
    global runningTasks
    try:runningTasks.remove(task.name) 
    except:pass
    print('completed task:', task.name)


import traceback
@celery_app.task(base=Singleton,name="bienci task")
def scrap_bienci_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = bienciScraper(payload)
    
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(base=Singleton,name="real estate logic-immo")
def scrap_logicimmo_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = LogicImmoScraper(payload)
    
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(base=Singleton,name="real estate Lefigaro")
def scrap_lefigaro_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = LefigaroScrapper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data

@celery_app.task(base=Singleton,name="real estate avendrealouer")
def scrap_avendrealouer_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = avendrealouerScrapper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(base=Singleton,name="real estate OuestFranceScrapper")
def scrap_OuestFranceScrapper_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = OuestFranceScrapper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data
@celery_app.task(base=Singleton,name="real estate gensdeconfianceScraper")
def scrap_gensdeconfiance_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        data = gensdeconfianceScraper(payload)
    except Exception as e:
        print(traceback.format_exc())
        print("Exception ================> ",e)
        data= "exception"
    # Scraping task obj start here
    print("Task End ================> ")
    return data


@celery_app.task(base=Singleton,name="real estate leboncoin")
def scrape_leboncoin_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:leboncoinAdScraper(payload)
    except Exception as e:
        traceback.print_exc()
        print("Exception ================> ",e)
    # Scraping task obj start here
    print("Scraper 3 ")

    print("Task End ================> ")

@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch leboncoin latest ad")
def update_leboncoin_ads():
    try:
        print("Task start ================> ")
        try:leboncoinAdScraper({
            "real_state_type":"Updated/Latest Ads"
        })
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
        print("Task End ================> ")
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="test task to")
def update_test_ads():
    try:
        print("Task start ================> ")
        try:
            print("sleeping...")
            time.sleep(1000)
            print("good morning")
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
        print("Task End ================> ")
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch paruvedu latest ad")
def update_paruvendu_ads():
    try:
        print("Task start ================> ")
        try:UpdateParuvendu()
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
        print("Task End ================> ")
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch pap latest ad")
def update_pap_ads():
    try:
        print("Task start ================> ")
        try:UpdatePap()
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
        print("Task End ================> ")
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch Bienci latest ad")
def update_Bienci_ads():
    try:
        print("Task start ================> ")
        try:
            data = UpdateBienci()
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
            data = "exeption"
        print("Task End ================> ")
        print(data)
        return data
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch logicImmo latest ad")
def update_logicImmo_ads():
    try:
        print("Task start ================> ")
        try:
            data = LogicImmoScraper({},update=True)
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
            data = "exeption"
        print("Task End ================> ")
        print(data)
        return data
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch lefigaro latest ad")
def update_lefigaro_ads():
    try:
        print("Task start ================> ")
        try:
            data = LefigaroScrapper({"real_state_type":"Updated/Latest Ads"})
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
            data = "exeption"
        print("Task End ================> ")
        print(data)
        return data
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch avendrealouer latest ad")
def update_avendrealouer_ads():
    try:
        print("Task start ================> ")
        try:
            data = avendrealouerScrapper({"real_state_type":"Updated/Latest Ads"})
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
            data = "exeption"
        print("Task End ================> ")
        print(data)
        return data
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate fetch seloger latest ad")
def update_seloger_ads():
    try:
        print("Task start ================> ")
        try:selogerScraper({},update=True)
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
        print("Task End ================> ")
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"

@celery_app.task(base=Singleton,name="real estate pap")
def scrape_pap_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    PapScraper(payload)

    # Scraping task obj start here

    print("Task End ================> ")
@celery_app.task(base=Singleton,name="real estate seloger")
def scrape_seloger_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    selogerScraper(payload)

    # Scraping task obj start here

    print("Task End ================> ")



@celery_app.task(base=Singleton,name="real estate paruvendu")
def scrape_paruvendu_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        ParuvenduScraper(payload)
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
    # Scraping task obj start here

    print("Task End ================> ")
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate OuestFrance")
def update_OuestFranceScrapper_ads():
    try:
        print("Task start ================> ")
        try:OuestFranceScrapper({},update=True)
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
        print("Task End ================> ")
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"
@celery_app.task(soft_time_limit=60*120,base=Singleton,name="real estate gensdeconfiance")
def update_gensdeconfianceScrapper_ads():
    try:
        print("Task start ================> ")
        try:gensdeconfianceScraper({},update=True)
        except Exception as e:
            traceback.print_exc()
            print("Exception ================> ",e)
        print("Task End ================> ")
    except SoftTimeLimitExceeded:
        print("Task End ================> ")
        return "revoked due to timeout"

@celery_app.task(base=Singleton,name="real estate green-acres")
def scrap_greenacres_task(payload):
    print("Task start ================> ")
    print("payload : ", payload)
    try:
        greenacresrScrapper(payload)
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
    # Scraping task obj start here

    print("Task End ================> ")


# recscrap active ad id
@celery_app.task(base=Singleton,name="rescrap bienci activeid")
def rescrap_bienciActiveId_task():
    try:
        rescrapActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap Avendrealouer activeid")
def rescrap_AvendrealouerActiveId_task():
    try:
        rescrapAvendrealouerActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap Pap activeid")
def rescrap_papActiveId_task():
    try:
        rescrapPapActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap paruvendu activeid")
def rescrap_paruvenduActiveId_task():
    try:
        rescrapParuvenduActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap Seloger activeid")
def rescrap_SelogerActiveId_task():
    try:
        rescrapSelogerActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap Lefigaro activeid")
def rescrap_LefigaroActiveId_task():
    try:
        rescrapLefigaroActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap logicImmo activeid")
def rescrap_logicImmoActiveId_task():
    try:
        rescraplogicImmoActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap Gensdeconfiance activeid")
def rescrap_GensdeconfianceActiveId_task():
    try:
        rescrapGensdeconfianceActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap OuestFrance activeid")
def rescrap_OuestFranceActiveId_task():
    try:
        rescrapOuestFranceActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap Leboncoin activeid")
def rescrap_LeboncoinActiveId_task():
    try:
        rescrapLeboncoinActiveId()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)
@celery_app.task(base=Singleton,name="rescrap missing data in elasticsearch")
def rescrap_updateMissinField_task():
    try:
        updateMissinField()
    except Exception as e:
        traceback.print_exc()
        print("Exception ==============>", e)

# rescrap_updateMissinField_task.apply_async()
# rescrap_bienciActiveId_task.apply_async()
# rescrap_papActiveId_task.apply_async()
# rescrap_paruvenduActiveId_task.apply_async()
# rescrap_SelogerActiveId_task.apply_async()
# rescrap_LefigaroActiveId_task.apply_async()
# rescrap_logicImmoActiveId_task.apply_async()
# rescrap_GensdeconfianceActiveId_task.apply_async()
# rescrap_OuestFranceActiveId_task.apply_async()
# rescrap_LeboncoinActiveId_task.apply_async()
# 4 website 1 Core = 4 Core CPU


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    print("rnnnint periodic tasks")
    # Calls update_leboncoin_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_leboncoin_ads.s(), name='update leboncoin ads in every 20 minuts')
    # recscrap active ad id leboncoin every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_LeboncoinActiveId_task.s(), name='checj leboncoin ads id in every week')
    # Calls update_peruvendu_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_paruvendu_ads.s(), name='update paruvendu ads every 20 minuts')
    # recscrap active ad id paruvendu every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_paruvenduActiveId_task.s(), name='checj paruvendu ads id in every week')
    # # Calls update_pap_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_pap_ads.s(), name='update pap ads every 20 minuts')
    # recscrap active ad id pap every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_papActiveId_task.s(), name='checj pap ads id in every week')
    # Calls update_seloger_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_seloger_ads.s(), name='update seloger ads every 20 minuts')
    # recscrap active ad id Seloger every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_SelogerActiveId_task.s(), name='checj Seloger ads id in every week')
    # Calls update_Bienci_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_Bienci_ads.s(), name='update Bienci ads every 20 minuts')
    # recscrap active ad id bienci every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_bienciActiveId_task.s(), name='checj bienci ads id in every week')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_logicImmo_ads.s(), name='update logicImmo ads every 20 minuts')
    # recscrap active ad id logicImmo every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_logicImmoActiveId_task.s(), name='checj logicImmo ads id in every week')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_lefigaro_ads.s(), name='update lefigaro ads every 20 minuts')
    # recscrap active ad id Lefigaro every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_LefigaroActiveId_task.s(), name='checj Lefigaro ads id in every week')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_avendrealouer_ads.s(), name='update avendrealouer ads every 20 minuts')
    # recscrap active ad id Avendrealouer every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_AvendrealouerActiveId_task.s(), name='checj Avendrealouer ads id in every week')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_OuestFranceScrapper_ads.s(), name='update OuestFranceScrapper ads every 20 minuts')
    # recscrap active ad id OuestFrance every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_OuestFranceActiveId_task.s(), name='checj OuestFrance ads id in every week')
    # Calls update_logicImmo_ads in every 20 minutes
    sender.add_periodic_task(20*60, update_gensdeconfianceScrapper_ads.s(), name='update gensdeconfiance ads every 20 minuts')
    # recscrap active ad id Gensdeconfiance every week
    sender.add_periodic_task(crontab(hour=7, minute=30, day_of_week=1), rescrap_GensdeconfianceActiveId_task.s(), name='checj Gensdeconfiance ads id in every week')
    # Calls check bienci active add every week
    sender.add_periodic_task(60*60*24, rescrap_updateMissinField_task.s(), name='rescrap missing fields in From source portal')
