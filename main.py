import asyncio
import datetime

import sqlalchemy
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, JSON
from sqlalchemy import String as String


from more_itertools import chunked

api_people = 'https://swapi.dev/api/people/'
artifact_list = [{'key': 'films', 'type': 'title'}, {'key': 'species', 'type': 'name'}, {'key': 'vehicles', 'type': 'name'}, {'key': 'starships', 'type': 'name'}]
art_hash_dict = {}


PG_DSN = 'postgresql+asyncpg://postgres:p@127.0.0.1:5431/asyncio_el'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)



CHUNK_SIZE = 14


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'{api_people}{people_id}') as response:
        json_data = await response.json()

    for art_dict in artifact_list:
        art_key = art_dict['key']
        artifact = json_data.get(art_key)
        art_type = art_dict['type']
        if artifact:
            coroutines_art = [get_artifact(artifacts=art, art_type=art_type, session=session) for art in artifact]
            artifact_res = await asyncio.gather(*coroutines_art)
            json_data[art_key] = ', '.join(artifact_res)
        else:
            json_data[art_key] = None

    print(f'end {people_id}')
    return json_data

async def get_artifact(artifacts, art_type: str, session: ClientSession):
    print(f'begin {artifacts}')
    art_hash = hash(artifacts)
    if art_hash in art_hash_dict:
        print("Есть!", art_hash_dict[art_hash])
        result = art_hash_dict[art_hash]
    else:
        async with session.get(f'{artifacts}') as response:
            json_data = await response.json()
        print(f'end {artifacts}')
        result = json_data[art_type]
        art_hash_dict[art_hash] = result
    return result

async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 85), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:

                print(item)
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(**{k: item[k] for k in ('id', 'birth_year', 'eye_color', 'films', 'gender', 'hair_color', 'height', 'homeworld', 'mass', 'name', 'skin_color', 'species', 'starships', 'vehicles') if k in item}) for item in people_chunk])
        await session.commit()

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}

    for task in tasks:
        await task


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)