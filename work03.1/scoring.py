import hashlib
import json
import random
import time


def get_score(store, phone, email, birthday=None, gender=None, first_name=None, last_name=None):
    if phone:
        try:
            phone = str(phone)
        except:
            phone = ''
    key_parts = [
        first_name or "",
        last_name or "",
        phone or "",
        birthday.strftime("%Y%m%d") if birthday is not None else "",
    ]
    key = "uid:" + hashlib.md5("".join(key_parts).encode()).hexdigest()
    # try get from cache,
    # fallback to heavy calculation in case of cache miss

    score = store.cache_get(key) or 0
    if score:
        return json.loads(score)
    if phone:
        score += 1.5
    if email:
        score += 1.5
    if birthday and gender:
        score += 1.5
    if first_name and last_name:
        score += 0.5
    # cache for 60 minutes
    store.cache_set(key, score, 60 * 60)
    return score


def get_interests(store, cid):
    key = "i:%s" % cid

    # -- запишем в базу случайные данные, чтобы потом их считать
    interests = ["cars", "pets", "travel", "hi-tech", "sport", "music", "books", "tv", "cinema", "geek", "otus"]
    res = random.sample(interests, cid)
    store.set(key, json.dumps(res))
    # --

    r = store.get(key)
    return json.loads(r) if r else []
