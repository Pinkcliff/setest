"""
Redis数据库交互模块
"""
import redis
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import config


class RedisDatabase:
    """Redis数据库操作类"""

    def __init__(self):
        """初始化Redis连接"""
        self.redis_client = redis.Redis(**config.REDIS_CONFIG)
        self._test_connection()

    def _test_connection(self):
        """测试Redis连接"""
        try:
            self.redis_client.ping()
            print("[OK] Redis连接成功")
        except redis.ConnectionError:
            print("[ERROR] Redis连接失败，请确保Redis服务已启动")
            raise

    def create_collection(self, name: str) -> str:
        """
        创建新的数据采集

        Args:
            name: 采集名称

        Returns:
            collection_id: 采集ID
        """
        collection_id = f"collection_{int(time.time() * 1000)}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 存储元数据
        meta_key = f"collection:meta:{collection_id}"
        self.redis_client.hset(meta_key, 'id', collection_id)
        self.redis_client.hset(meta_key, 'name', name)
        self.redis_client.hset(meta_key, 'created_at', timestamp)
        self.redis_client.hset(meta_key, 'status', 'collecting')

        # 添加到采集列表
        self.redis_client.sadd("collections:list", collection_id)

        return collection_id

    def save_sample_data(self, collection_id: str, data: Dict[str, Any]) -> bool:
        """
        保存单次采样数据

        Args:
            collection_id: 采集ID
            data: 采样数据字典

        Returns:
            是否保存成功
        """
        try:
            timestamp = data['timestamp']
            ts_key = f"collection:{collection_id}:timestamps"

            # 添加时间戳到有序集合
            self.redis_client.zadd(ts_key, {str(timestamp): timestamp})

            # 存储数据
            data_key = f"collection:{collection_id}:data:{timestamp}"
            self.redis_client.hset(data_key, 'fans', json.dumps(data['fans']))
            self.redis_client.hset(data_key, 'temp_sensors', json.dumps(data['temp_sensors']))
            self.redis_client.hset(data_key, 'wind_speed_sensors', json.dumps(data['wind_speed_sensors']))
            self.redis_client.hset(data_key, 'temp_humidity_sensors', json.dumps(data['temp_humidity_sensors']))
            self.redis_client.hset(data_key, 'pressure_sensor', json.dumps(data['pressure_sensor']))

            return True
        except Exception as e:
            print(f"保存数据失败: {e}")
            return False

    def finish_collection(self, collection_id: str):
        """
        标记采集完成

        Args:
            collection_id: 采集ID
        """
        self.redis_client.hset(
            f"collection:meta:{collection_id}",
            'status',
            'completed'
        )

    def get_collections_list(self) -> List[Dict[str, str]]:
        """
        获取所有采集列表

        Returns:
            采集信息列表
        """
        collection_ids = self.redis_client.smembers("collections:list")
        collections = []

        for cid in collection_ids:
            meta = self.redis_client.hgetall(f"collection:meta:{cid}")
            if meta:
                # 获取数据点数量
                count = self.redis_client.zcard(f"collection:{cid}:timestamps")
                meta['sample_count'] = str(count)
                collections.append(meta)

        return sorted(collections, key=lambda x: x['created_at'], reverse=True)

    def get_collection_data(self, collection_id: str, start: int = 0, end: int = -1) -> List[Dict]:
        """
        获取指定采集的数据

        Args:
            collection_id: 采集ID
            start: 起始索引
            end: 结束索引（-1表示到最后）

        Returns:
            数据列表
        """
        ts_key = f"collection:{collection_id}:timestamps"

        # 获取时间戳列表（支持分页）
        if end == -1:
            timestamps = self.redis_client.zrange(ts_key, start, -1)
        else:
            timestamps = self.redis_client.zrange(ts_key, start, end)

        data_list = []
        for ts in timestamps:
            ts_float = float(ts)
            data_key = f"collection:{collection_id}:data:{ts_float}"
            data = self.redis_client.hgetall(data_key)

            if data:
                data_list.append({
                    'timestamp': ts_float,
                    'fans': json.loads(data['fans']),
                    'temp_sensors': json.loads(data['temp_sensors']),
                    'wind_speed_sensors': json.loads(data['wind_speed_sensors']),
                    'temp_humidity_sensors': json.loads(data['temp_humidity_sensors']),
                    'pressure_sensor': json.loads(data['pressure_sensor'])
                })

        return data_list

    def get_collection_meta(self, collection_id: str) -> Optional[Dict[str, str]]:
        """
        获取采集元数据

        Args:
            collection_id: 采集ID

        Returns:
            元数据字典
        """
        return self.redis_client.hgetall(f"collection:meta:{collection_id}")

    def delete_collection(self, collection_id: str) -> bool:
        """
        删除指定采集

        Args:
            collection_id: 采集ID

        Returns:
            是否删除成功
        """
        try:
            # 获取所有时间戳
            ts_key = f"collection:{collection_id}:timestamps"
            timestamps = self.redis_client.zrange(ts_key, 0, -1)

            # 删除所有数据
            for ts in timestamps:
                ts_float = float(ts)
                data_key = f"collection:{collection_id}:data:{ts_float}"
                self.redis_client.delete(data_key)

            # 删除时间戳集合和元数据
            self.redis_client.delete(ts_key)
            self.redis_client.delete(f"collection:meta:{collection_id}")
            self.redis_client.srem("collections:list", collection_id)

            return True
        except Exception as e:
            print(f"删除失败: {e}")
            return False

    def get_sample_count(self, collection_id: str) -> int:
        """获取采集的数据点数量"""
        return self.redis_client.zcard(f"collection:{collection_id}:timestamps")
