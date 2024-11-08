import pandas as pd
import requests
import logging
import random

logging.basicConfig(filename='./test_service.log',level=logging.INFO)

# задаём адрес сервиса рекомендаций и параметры запросов
service_url = 'http://127.0.0.1:8010'
headers = {
    'Content-type': 'application/json', 
    'Accept': 'text/plain'
    }

# подгружаем данные для создания запросов
evenst = pd.read_parquet('./data/events.parquet')
items = pd.read_parquet('./data/items.parquet')
names = pd.read_parquet('./data/catalog_names.parquet')

track_names = names[names['type'] == 'track']

def resp_for_logging(resp):
    '''
    обработка отвера сервиса рекомендаций
    '''
    
    if resp.status_code == 200:
        recs = resp.json()
    else:
        recs = []
        print(f"status code: {resp.status_code}")
    
    return recs

def track_names_from_ids_list(ids):
    '''
    вывод названий треков
    '''
    
    return track_names[track_names['id'].isin(ids)][['id','name']].set_index('id')

print("завершена инициализация таблиц для тестирования")
logging.info(f"завершена инициализация таблиц для тестирования")
# выбираем случайного пользователя
user_id = evenst['user_id'].sample(1).iloc[0]

logging.info(f"История пользователя {user_id}. Случайные 10 треков")

history = evenst[evenst['user_id']==user_id]['item_id'].sample(10)
names = track_names_from_ids_list(history)
logging.info(names)


logging.info(f"Запрос рекомендаций для пользователя {user_id}")
req = {
    'user_id' : user_id,
    'k' : 10
}
logging.info(req)

resp = requests.post(service_url + '/recommendations', headers=headers, params=req)
recs = resp_for_logging(resp)

logging.info(recs)
names = track_names_from_ids_list(recs['recs'])
logging.info(names)

# берём случайные 5 треков из рекомендованного набора
track_ids = random.sample(recs['recs'],k=5)
for item in track_ids:
    # пишем в историю пользователю взаимодействие с треком
    req = {
        'user_id' : user_id,
        'item_id' : item
    }
    logging.info(req)
    resp = requests.post(service_url + '/events_put', headers=headers, params=req)
    info = resp_for_logging(resp)
    logging.info(info)
    # смотрим как изменилась рекомендация после прослушивания очередного трека
    req = {
        'user_id' : user_id,
        'k' : 10
    }
    logging.info(req)
    resp = requests.post(service_url + '/recommendations', headers=headers, params=req)
    recs = resp_for_logging(resp)
    logging.info(recs)
    names = track_names_from_ids_list(recs['recs'])
    logging.info(names)

###
# рекомендации для пользователя без персональных рекомендаций
###

user_id = evenst['user_id'].max() + 1
logging.info(f"рекомендации для \"холодного\" пользователя (без персональных рекомендаций)")

logging.info(f"Запрос рекомендаций для пользователя {user_id}")
req = {
    'user_id' : user_id,
    'k' : 10
}
logging.info(req)

resp = requests.post(service_url + '/recommendations', headers=headers, params=req)
recs = resp_for_logging(resp)

logging.info(recs)
names = track_names_from_ids_list(recs['recs'])
logging.info(names)

# берём случайные 5 треков из рекомендованного набора
track_ids = random.sample(recs['recs'],k=5)
for item in track_ids:
    # пишем в историю пользователю взаимодействие с треком
    req = {
        'user_id' : user_id,
        'item_id' : item
    }
    logging.info(req)
    resp = requests.post(service_url + '/events_put', headers=headers, params=req)
    info = resp_for_logging(resp)
    logging.info(info)
    # смотрим как изменилась рекомендация после прослушивания очередного трека
    req = {
        'user_id' : user_id,
        'k' : 10
    }
    logging.info(req)
    resp = requests.post(service_url + '/recommendations', headers=headers, params=req)
    recs = resp_for_logging(resp)
    logging.info(recs)
    names = track_names_from_ids_list(recs['recs'])
    logging.info(names)

print("Тестирование завершено, результаты записаны в LOG")