import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
from recsys_heandler import RecSysHeandler
from events_store import EventStore
from rec_ranks import RecRanks
import random

rec_store = RecSysHeandler()
events_store = EventStore()
rec_rank = RecRanks()

#logging.basicConfig(filename='./service/logs/fastapi.log',level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logging.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    rec_store.stats()
    logging.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    # вернёт k персональных рекомендаций если они подготовлены offline
    # или k популярных треков
    personal = rec_store.personal_rec(user_id,k*3)
    events = events_store.get(user_id, k)
    if len(events) == 0:
        # отдаём рекомендации для пользователя без истории
        recs = rec_rank.calc_ranks(user_id,personal)[:k]
    else:
        # для пользователей с историей запрашивает по 10 похожих треков
        recs_online = []
        for item_id in events:
            recs_online += rec_store.items_rec(item_id,k*3) 
        # убираем из рекомендаций треки, которые пользователь слушал и дубли от персональных/популярных
        recs_online = list(set(recs_online) - set(events) - set(personal))
        # поскольку score для треков посчитан как степень похожести на каждый из отдельных item_id 
        # и ни как не сопостамваим для разных треков, то ранжировать их между собой можно либо случайным образом
        # либо взвешивать треки на жанровый, альбомный и исполнительский рейтинг для каждого пользователя
        # такая реализация вполне адекватна, но может работать медленно. т.к. взаимодействуетс большими таблицами events и items
        
        all_recs_for_rank = recs_online + personal
        
        #recs = random.sample(all_recs_for_rank,k=k)
        recs = rec_rank.calc_ranks(user_id,all_recs_for_rank)[:k]

    return {"recs": recs}

@app.post("/events_put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """

    events_store.put(user_id, item_id)

    return {"result": "ok"}