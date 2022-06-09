from asyncio import tasks
from urllib import response
import urllib.request
import urllib.parse
import aiohttp
import asyncio
from tqdm import tqdm
import json
try:
    from getfiterparam import getFilter
except:
    from .getfiterparam import getFilter
from pytest import param
try:
    from uploader import AsyncKafkaTopicProducer
except:
    from .uploader import AsyncKafkaTopicProducer
from kafka_publisher import KafkaTopicProducer

pagesize  = 100 # maxsize is 100
producer = KafkaTopicProducer()
url = "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"

async def fetch(session,url,params = None,method="get",**kwargs):
    if params:
        query_string = urllib.parse.urlencode( params )
        url += "?"+query_string 
    try:
        res = await session.get(url,params=params)
    except Exception as e:
        await asyncio.sleep(3)
        return await fetch(session,url,params,method,**kwargs)
    response = await res.json()
    return response

async def savedata(resjson,**kwargs):
    resstr = ''
    ads = resjson['feed']["row"]
    # producer = kwargs["producer"]
    for ad in ads:
        producer.kafka_producer_sync(topic="paruvendu-data_v1", data=ad)
    # await producer.TriggerPushDataList('paruvendu-data_v1',ads)
    # for ad in ads:
    #     resstr += json.dumps(ad)+"\n"
    # with open("output.json",'a') as file:
    #     file.write(resstr)
    # print('saved data')s
async def startCrawling(session,filterParamList,**kwargs):
    for param in filterParamList:
        param['showdetail'] = 1
        param["itemsPerPage"] = pagesize
        data = await fetch(session,url,param)
        # data= json.load(res)
        await savedata(data,**kwargs)
        totalres = int(data["feed"]["@totalResults"])
        totalpage = totalres/pagesize
        totalpage = int(totalpage) if totalpage==int(totalpage) else int(totalpage)+1
        print(totalres,param)
        tasks = []
        for i in tqdm(range(2,totalpage+1)):
            param['p'] = i
            tasks.append(asyncio.ensure_future(parstItems(session,param,**kwargs)))
        await asyncio.gather(*tasks)  
async def parstItems(session,param,**kwargs):
    data = await fetch(session,url,param)
    # data = json.load(res)
    await savedata(data,**kwargs)
async def main(adsType = ""):
    # catid info
    # IVH00000 is for  Vente immobilier 
    # ILH00000 is for Location immobilier
    if adsType == "sale":
        catid = "IVH00000"
    else:
        catid = "ILH00000"
    params = [
    {
        'ver':'4.1.4',
        'itemsPerPage':'12',
        'mobId':'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd',
        'p':'1',
        'catId':catid,
        'filters[_R1]':catid,
        'sortOn':'dateMiseEnLigne',
        'sortTo':'DESC',
        'key':'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh',
    },
    ]
    # filterParamList = [*getFilter(param) for param in params
    filterParamList = []
    for param in params:
        filterParamList+=getFilter(param)
    # filterParamList = [{'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 0, 'filters[P5M1]': 33919}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 33920, 'filters[P5M1]': 50880}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 50881, 'filters[P5M1]': 68689}, 
    # {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 
    # 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 68690, 'filters[P5M1]': 87307}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 87308, 'filters[P5M1]': 106080}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 106081, 'filters[P5M1]': 128889}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 128890, 'filters[P5M1]': 
    # 147437}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 147438, 'filters[P5M1]': 161225}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 161226, 'filters[P5M1]': 176302}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 176303, 'filters[P5M1]': 188953}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 188954, 'filters[P5M1]': 200486}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 200487, 'filters[P5M1]': 214870}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 214871, 'filters[P5M1]': 227984}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 227985, 'filters[P5M1]': 239478}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 239479, 'filters[P5M1]': 251552}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 251553, 'filters[P5M1]': 265522}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 265523, 'filters[P5M1]': 281726}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 281727, 'filters[P5M1]': 298920}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': 
    # '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 298921, 'filters[P5M1]': 317163}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 317164, 'filters[P5M1]': 339917}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 
    # 'filters[P5M0]': 339918, 'filters[P5M1]': 364304}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 
    # 0, 'filters[P5M0]': 364305, 'filters[P5M1]': 394385}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 394386, 'filters[P5M1]': 431262}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 431263, 'filters[P5M1]': 483506}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 483507, 'filters[P5M1]': 570012}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 570013, 'filters[P5M1]': 746663}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 746664, 'filters[P5M1]': 1639784}, {'ver': '4.1.4', 'itemsPerPage': 1, 'mobId': 'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd', 'p': '1', 'catId': 'IVH00000', 'filters[_R1]': 'IVH00000', 'sortOn': 'prix', 'sortTo': 'DESC', 'key': 'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh', 'showdetail': 0, 'filters[P5M0]': 1639785, 'filters[P5M1]': 317617372}]
    async with aiohttp.ClientSession() as session:
        producer = AsyncKafkaTopicProducer()
        await startCrawling(session,filterParamList,producer=producer)
        await producer.stopProducer()
def main_scraper(paylaod):
    # paylaod = {'text': 'house', 'min_price': 0.0, 'max_price': 0.0, 'city': 'string', 'rooms': 0,
    #            'real_state_type': 'rental'}
    # if paylaod["real_state_type"] == "sale":
    #     print("sales code is working")
    #     asyncio.run(scrape_sale_ads(paylaod["min_price"], paylaod["max_price"], paylaod["text"]))

    # elif paylaod["real_state_type"] == "rental":
    #     print("real code is working")
    #     asyncio.run(scrape_rental_ads(0, 0, ''))
    asyncio.run(main(paylaod["real_state_type"]))
if __name__=="__main__":
    url = "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"
    asyncio.run(main())

