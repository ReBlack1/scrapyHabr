import aiohttp
import asyncio

import schedule

import psycopg2
import json

from bs4 import BeautifulSoup

from datetime import datetime


# запись результатов в базу данных
def insert_db(topics_list):
    # подключение к бд (бд имеет одну таблицу topics(id, habr_id, title, text))
    con = psycopg2.connect(
        database="habrtopics",
        user="habrroot",
        password="qwerty",
        host="127.0.0.1",
        port="5432"
    )
    print("База данных успешно открыта!")
    cur = con.cursor()
    cur.execute("SELECT habr_id from topics")
    db_data = cur.fetchall()
    habr_ids = [habr_id[0] for habr_id in db_data]
    topics = list({topic['habr_id']: topic for topic in topics_list if topic['habr_id'] not in habr_ids}.values())

    for topic in topics:
        try:
            cur.execute(
                "INSERT INTO topics (habr_id,title,text) VALUES (%s, %s, %s)",
                (topic['habr_id'], topic['title'], topic['text'])
            )
        except:
            print("Статья c id %s уже существует!" % topic['habr_id'])

    con.commit()
    print("Записи вставлены успешно!")

    con.close()


async def fetch(session, url):
    async with session.get(url,) as response:
        return await response.text(errors='ignore')


# парсинг страниц со статьями (id, заголовка и текста)
async def parse_topics(urls_topics):
    async with aiohttp.ClientSession() as session:
        topics = [await fetch(session, url) for url in urls_topics]
        topics_list = []
        for topic in topics:
            soup = BeautifulSoup(topic)
            topic_id = soup.find('article', {'class': 'post_full'}).get('id')
            topic_title = soup.find('span', {'class': 'post__title-text'}).get_text()
            topic_text = soup.find('div', {'class': 'post__body_full'}).get_text()
            topics_list.append({
                'habr_id': topic_id,
                'title': topic_title,
                'text': topic_text
            })
        insert_db(topics_list)


# парсинг страницы со списокм статей (посик url статей)
async def parse_list(sources):
    async with aiohttp.ClientSession() as session:
        pages = [await fetch(session, page) for page in sources]
        urls_topics = []
        for page in pages:
            soup = BeautifulSoup(page)
            topic_link = soup.find_all('a', {'class': 'post__title_link'})
            for a in topic_link:
                urls_topics.append(a.get('href'))
        await parse_topics(urls_topics)


# работа которая выполняется раз в 8 часов
def job():
    start_time = datetime.now()
    print('Начал работу в ', start_time)
    with open('sources.json') as f:
        templates = json.load(f)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(parse_list(templates["habr"]))
    end_time = datetime.now()
    print('Закончил работу в ', end_time)
    print('Затратил на работу ', end_time - start_time)


job()  # первый запуск работы
schedule.every(8).hours.do(job)  # запускать работу каждые 8 часов

while True:
    schedule.run_pending()
