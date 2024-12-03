import logging
from fastapi import APIRouter, Path
from bson.objectid import ObjectId

from extensions import cache, mongo, s3, ch
from .models import BookModel, UpdateBookModel, S3ResponseModel

router = APIRouter(prefix='/test')
logger = logging.getLogger()


@router.get('/redis/set', tags=['Redis'])
async def redis_get(key: str, val: str):
    await cache.default.set(key, val)


@router.get('/redis/get', tags=['Redis'])
async def redis_get(key: str):
    v = await cache.default.get(key)
    return {'detail': v}


@router.post('/mongo/upload', description='upload your book info', tags=['Mongo'])
async def mongo_insert(book: BookModel):
    result = await mongo.default.store.book.insert_one(book.dict())
    return {'message': f'{str(result.inserted_id)}', 'status': 'ok'}


@router.get('/mongo/{book_id}', description='show book', tags=['Mongo'])
async def mongo_get(book_id: str = Path(description="the book id")):
    result = await mongo.default.store.book.find_one({'_id': ObjectId(book_id)})
    result['_id'] = str(result['_id'])
    return {'data': result or {}, 'status': 'ok'}


@router.put('/mongo/{book_id}', description='modify book info', tags=['Mongo'])
async def mongo_update(book_id: str, update_info: UpdateBookModel):
    result = await mongo.default.store.book.update_one(
        {'_id': ObjectId(book_id)},
        {'$set': update_info.dict(exclude_none=True)}
    )

    return {'data': result.raw_result, 'status': 'ok'}


@router.get('/s3/list',
            description='list files within bucket',
            response_model=S3ResponseModel,
            tags=['S3'])
async def s3_list():

    async with s3.default as client:
        result = await client.list_objects(Bucket='hello-s3')
        files = []
        for file in result.get('Contents', []):
            files.append({'name': file['Key'], 'size': file['Size']})

        return {'data': files, 'status': 'ok'}


@router.get('/ch/list', description='show table rows', tags=['Clickhouse'])
async def ch_list():
    async with ch.default as client:
        async for row in client.iterate(
                "select * from system.tables"
        ):
            logger.info(row[0])

    return {'data': '', 'status': 'ok'}
