"""
Redis 到 MongoDB 数据同步程序
"""
import sys
import json
import time
from datetime import datetime
from typing import List, Dict, Any
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm

import redis_db
import config


class MongoSync:
    """MongoDB 同步类"""

    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/", db_name: str = "sensor_data"):
        """
        初始化 MongoDB 连接

        Args:
            mongo_uri: MongoDB 连接字符串
            db_name: 数据库名称
        """
        try:
            self.client = MongoClient(mongo_uri)
            self.db = self.client[db_name]

            # 测试连接
            self.client.admin.command('ping')
            print(f"[OK] MongoDB 连接成功 (数据库: {db_name})")
        except Exception as e:
            print(f"[ERROR] MongoDB 连接失败: {e}")
            print("请确保 MongoDB 服务已启动")
            raise

        # 集合名称
        self.collections_col = self.db["collections"]
        self.samples_col = self.db["samples"]

        # 创建索引
        self._create_indexes()

        self.redis_db = redis_db.RedisDatabase()

    def _create_indexes(self):
        """创建索引以提高查询性能"""
        print("[INFO] 创建索引...")

        # collections 集合索引
        self.collections_col.create_index("collection_id", unique=True)
        self.collections_col.create_index("created_at")
        self.collections_col.create_index("status")

        # samples 集合索引
        self.samples_col.create_index([("collection_id", 1), ("timestamp", 1)], unique=True)
        self.samples_col.create_index("collection_id")
        self.samples_col.create_index("timestamp")

        print("[OK] 索引创建完成")

    def sync_collection_meta(self, collection_id: str) -> bool:
        """
        同步单个采集的元数据

        Args:
            collection_id: 采集ID

        Returns:
            是否同步成功
        """
        try:
            meta = self.redis_db.get_collection_meta(collection_id)
            if not meta:
                return False

            # 获取样本数量
            sample_count = self.redis_db.get_sample_count(collection_id)

            # 构建 MongoDB 文档
            doc = {
                "collection_id": collection_id,
                "name": meta.get('name', ''),
                "created_at": meta.get('created_at', ''),
                "status": meta.get('status', ''),
                "sample_count": sample_count,
                "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            # 使用 upsert 更新或插入
            self.collections_col.update_one(
                {"collection_id": collection_id},
                {"$set": doc},
                upsert=True
            )

            return True
        except Exception as e:
            print(f"[ERROR] 同步元数据失败 ({collection_id}): {e}")
            return False

    def sync_collection_samples(self, collection_id: str, batch_size: int = 1000) -> int:
        """
        同步单个采集的所有样本数据

        Args:
            collection_id: 采集ID
            batch_size: 批量写入大小

        Returns:
            同步的样本数量
        """
        try:
            # 获取总样本数
            total_count = self.redis_db.get_sample_count(collection_id)

            if total_count == 0:
                print(f"[INFO] 采集 {collection_id} 没有数据")
                return 0

            print(f"[INFO] 开始同步采集 {collection_id} 的数据 ({total_count} 条)...")

            synced_count = 0
            batch_operations = []

            # 使用 tqdm 显示进度条
            with tqdm(total=total_count, desc=f"同步 {collection_id[:20]}...", unit="条") as pbar:
                offset = 0
                while offset < total_count:
                    # 批量获取数据
                    data_list = self.redis_db.get_collection_data(
                        collection_id,
                        offset,
                        offset + batch_size - 1
                    )

                    if not data_list:
                        break

                    # 构建批量操作
                    for data in data_list:
                        doc = {
                            "collection_id": collection_id,
                            "timestamp": data['timestamp'],
                            "datetime": datetime.fromtimestamp(data['timestamp']).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                            "fans": data['fans'],
                            "temp_sensors": data['temp_sensors'],
                            "wind_speed_sensors": data['wind_speed_sensors'],
                            "temp_humidity_sensors": data['temp_humidity_sensors'],
                            "pressure_sensor": data['pressure_sensor']
                        }

                        batch_operations.append(
                            UpdateOne(
                                {
                                    "collection_id": collection_id,
                                    "timestamp": data['timestamp']
                                },
                                {"$set": doc},
                                upsert=True
                            )
                        )

                        synced_count += 1
                        pbar.update(1)

                    # 批量写入
                    if batch_operations:
                        self.samples_col.bulk_write(batch_operations, ordered=False)
                        batch_operations.clear()

                    offset += len(data_list)

            print(f"[OK] 采集 {collection_id} 同步完成 ({synced_count} 条)")
            return synced_count

        except Exception as e:
            print(f"[ERROR] 同步样本数据失败 ({collection_id}): {e}")
            return 0

    def sync_all(self, delete_missing: bool = False) -> Dict[str, Any]:
        """
        同步所有采集数据

        Args:
            delete_missing: 是否删除 MongoDB 中存在但 Redis 中不存在的数据

        Returns:
            同步结果统计
        """
        print("\n" + "="*50)
        print("开始同步 Redis -> MongoDB")
        print("="*50 + "\n")

        start_time = time.time()
        result = {
            "collections_synced": 0,
            "samples_synced": 0,
            "errors": []
        }

        try:
            # 获取所有采集列表
            collections = self.redis_db.get_collections_list()

            if not collections:
                print("[INFO] Redis 中没有采集数据")
                return result

            print(f"[INFO] 发现 {len(collections)} 个采集记录\n")

            # 同步每个采集
            for coll in collections:
                collection_id = coll['id']

                # 同步元数据
                if self.sync_collection_meta(collection_id):
                    result["collections_synced"] += 1
                else:
                    result["errors"].append(f"元数据同步失败: {collection_id}")
                    continue

                # 同步样本数据
                sample_count = self.sync_collection_samples(collection_id)
                result["samples_synced"] += sample_count

                print()  # 空行分隔

            # 可选：删除 MongoDB 中多余的数据
            if delete_missing:
                self._delete_missing_collections(collections)

        except Exception as e:
            result["errors"].append(f"同步过程出错: {str(e)}")
            print(f"[ERROR] 同步过程出错: {e}")

        # 打印统计结果
        elapsed_time = time.time() - start_time
        print("\n" + "="*50)
        print("同步完成")
        print("="*50)
        print(f"采集记录: {result['collections_synced']} 个")
        print(f"样本数据: {result['samples_synced']} 条")
        print(f"耗时: {elapsed_time:.2f} 秒")

        if result["errors"]:
            print(f"\n错误 ({len(result['errors'])}):")
            for error in result["errors"]:
                print(f"  - {error}")

        return result

    def _delete_missing_collections(self, redis_collections: List[Dict]):
        """删除 MongoDB 中存在但 Redis 中不存在的数据"""
        redis_ids = {coll['id'] for coll in redis_collections}

        # 查找 MongoDB 中的所有 collection_id
        mongo_ids = set(self.collections_col.distinct("collection_id"))

        # 找出需要删除的
        to_delete = mongo_ids - redis_ids

        if to_delete:
            print(f"\n[INFO] 删除 MongoDB 中多余的 {len(to_delete)} 个采集...")
            for coll_id in to_delete:
                # 删除样本数据
                self.samples_col.delete_many({"collection_id": coll_id})
                # 删除元数据
                self.collections_col.delete_one({"collection_id": coll_id})
            print(f"[OK] 已删除 {len(to_delete)} 个多余的采集")

    def get_sync_status(self) -> Dict[str, Any]:
        """获取同步状态"""
        redis_collections = self.redis_db.get_collections_list()
        mongo_collections = list(self.collections_col.find({}, {"_id": 0, "collection_id": 1, "sample_count": 1}))

        redis_ids = {coll['id'] for coll in redis_collections}
        mongo_ids = {coll['collection_id'] for coll in mongo_collections}

        return {
            "redis_collections": len(redis_collections),
            "mongo_collections": len(mongo_collections),
            "synced": len(redis_ids & mongo_ids),
            "missing_in_mongo": len(redis_ids - mongo_ids),
            "extra_in_mongo": len(mongo_ids - redis_ids)
        }

    def close(self):
        """关闭连接"""
        self.client.close()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="Redis 到 MongoDB 数据同步工具")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017/",
                        help="MongoDB 连接字符串")
    parser.add_argument("--db-name", default="sensor_data",
                        help="MongoDB 数据库名称")
    parser.add_argument("--delete-missing", action="store_true",
                        help="删除 MongoDB 中存在但 Redis 中不存在的数据")
    parser.add_argument("--status", action="store_true",
                        help="仅显示同步状态")

    args = parser.parse_args()

    try:
        sync = MongoSync(mongo_uri=args.mongo_uri, db_name=args.db_name)

        if args.status:
            # 仅显示状态
            status = sync.get_sync_status()
            print("\n同步状态:")
            print(f"  Redis 采集数: {status['redis_collections']}")
            print(f"  MongoDB 采集数: {status['mongo_collections']}")
            print(f"  已同步: {status['synced']}")
            print(f"  未同步: {status['missing_in_mongo']}")
            print(f"  多余数据: {status['extra_in_mongo']}")
        else:
            # 执行同步
            sync.sync_all(delete_missing=args.delete_missing)

        sync.close()

    except KeyboardInterrupt:
        print("\n\n[INFO] 用户中断")
    except Exception as e:
        print(f"\n[ERROR] 程序异常: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
