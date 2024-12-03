from pydantic import BaseModel
from typing import Any
from extensions import mongo, cache


class TableSelector:

    class Meta:
        db_driver = mongo

    @classmethod
    def get_conn(cls, name='default'):
        return getattr(cls.Meta.db_driver, name)

    @classmethod
    def get_prefix_alloc_table(cls, v, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        if str(v) == '4':
            return db.vis.vis_ipv4_alloc
        else:
            return db.vis.vis_ipv6_alloc

    @classmethod
    def get_trend_table(cls, v, name='default'):
        db = getattr(cls.Meta.db_driver, name)

        if str(v) == '4':
            return db.vis.vis_ipv4_country_trend
        else:
            return db.vis.vis_ipv6_country_trend

    @classmethod
    def get_asn_alloc_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_asn_alloc

    @classmethod
    def get_edu_as_history_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_edu_as_history

    @classmethod
    def get_edu_as_city_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_edu_as_city

    @classmethod
    def get_as_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_as

    @classmethod
    def get_as_hijack_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_as_hijack_event

    @classmethod
    def get_edu_as_path_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_edu_as_path

    @classmethod
    def get_as_simple_hijack_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_as_simple_hijack_event

    @classmethod
    def get_as_cache_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_as_cache

    @classmethod
    def get_ip_netflow_table(cls, name='default'):
        db = getattr(cls.Meta.db_driver, name)
        return db.vis.vis_ip_netflow

    @classmethod
    def get_ip_map_table(cls, name='default'):
        return cls.get_conn(name).vis.vis_ip_map

    @classmethod
    def get_ip_picture(cls, v, name='default'):
        conn = cls.get_conn(name)
        if str(v) == '4':
            return conn.vis.vis_ipv4_picture
        return conn.vis.vis_ipv6_picture

    @classmethod
    def get_ip_space(cls, v, name='default'):
        conn = cls.get_conn(name)
        if str(v) == '4':
            return conn.vis.vis_ipv4_space
        return conn.vis.vis_ipv6_space

    @classmethod
    def get_ip_trend(cls, name='default'):
        conn = cls.get_conn(name)
        return conn.vis.vis_ip_trend


class CacheSelector:
    class Meta:
        cache_driver = cache

    @classmethod
    def get_cache(cls, name='default'):
        conn = getattr(cls.Meta.cache_driver, name)
        return conn


class VisCache(BaseModel):
    key: str
    value: Any

    @classmethod
    async def get_cache(cls, key: str):
        _table = TableSelector.get_as_cache_table()
        cached = await _table.find_one({'key': key}, {'_id': 0, 'key': 0})
        return cached

    @classmethod
    async def add_cache(cls, key: str, val: Any):
        _table = TableSelector.get_as_cache_table()
        await _table.update_one({'key': key}, {'$set': val}, upsert=True)
