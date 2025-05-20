#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import wiredtiger
from typing import Optional, List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WiredTigerReader:
    def __init__(self, data_dir: str):
        """
        初始化WiredTiger读取器

        Args:
            data_dir: MongoDB数据目录路径
        """
        self.data_dir = data_dir
        self.conn = None
        self.session = None # 新增 session 属性

    def connect(self) -> bool:
        """
        连接到WiredTiger数据库

        Returns:
            bool: 连接是否成功
        """
        try:
            self.conn = wiredtiger.wiredtiger_open(
                self.data_dir,
                # 建议添加 error_prefix 参数以便更好地定位错误
                "create=false,readonly=true,error_prefix='WiredTigerReader: '"
            )
            # 创建 session
            self.session = self.conn.open_session()
            logger.info("成功连接到WiredTiger数据库并打开会话")
            return True
        except Exception as e:
            logger.error(f"连接WiredTiger数据库失败: {str(e)}")
            return False

    def list_collections(self) -> List[str]:
        """
        列出所有集合

        Returns:
            List[str]: 集合名称列表
        """
        if not self.session: # 检查 session 是否存在
            logger.error("未连接到数据库或会话未打开")
            return []

        try:
            # 通过 self.session 打开游标
            # WiredTiger 的元数据表通常是 "metadata:collection" 或类似的，
            # 但 MongoDB 使用的可能是 "table:collection" 来存储集合名称元数据
            # 或者更准确地，是通过查询 "table:_mdb_catalog" 或类似表来获取集合列表。
            # 直接使用 "table:collection" 可能不是标准方式来列出所有用户集合。
            # WiredTiger 本身并不直接等同于 MongoDB 的集合概念。
            # MongoDB 在 WiredTiger 之上构建了自己的结构。
            # 通常，要列出 MongoDB 集合，需要查看特定的元数据表，
            # 例如 _mdb_catalog 或类似名称。
            # 如果 "table:collection" 确实是你环境中存储集合元数据的表，则保留。
            # 否则，你可能需要找到正确的元数据表名。

            # 一个更通用的方法是遍历所有以 "table:" 开头的 URI
            # 但这需要不同的 API 调用，比如 conn.get_uri_iterator()
            # 这里我们先假设 "table:collection" 是一个特殊表用于此目的，
            # 或者用户知道其 MongoDB 配置如何将集合映射到 WiredTiger 表。

            # 假设 "table:collection" 是对的，或者需要更具体的表名
            # 根据 WiredTiger 的文档，列出所有表通常是通过查询 "metadata:" URI。
            # 然后筛选出用户表。
            # 例如，可以打开 "metadata:" 游标，然后遍历其中的键。
            # 但这里的 "table:collection" 可能是特定于 MongoDB 如何使用 WiredTiger。

            # 为了更准确地列出 MongoDB 集合，通常需要解析 `_mdb_catalog` 这样的表。
            # 不过，按照你原来的代码逻辑，我们先修改为使用 session：
            meta_cursor = self.session.open_cursor('metadata:', None, None)
            collections = []
            for uri, conf_string in meta_cursor:
                if uri.startswith("table:") and not uri.startswith("table:index-") and not uri.startswith("table:sizeStorer"):
                     # 从 "table:yourDb.yourCollection" 中提取 "yourDb.yourCollection"
                    collection_name_with_db = uri.split(":", 1)[1]
                    # 如果你的集合名不包含数据库前缀，则可能需要进一步处理
                    collections.append(collection_name_with_db)
            meta_cursor.close()

            # 如果你确定 "table:collection" 是一个特殊的表，包含所有集合名作为键，
            # 那么你的原始逻辑，在修正了 session 调用后，可能是这样的：
            # cursor = self.session.open_cursor("table:collection", None, None)
            # collections = []
            # while True:
            #     try:
            #         ret = cursor.next()
            #         if ret != 0: # WT_NOTFOUND or error
            #             break
            #         collections.append(cursor.get_key())
            #     except wiredtiger.WiredTigerError as e:
            #         if "WT_NOTFOUND" in str(e): # 明确检查 WT_NOTFOUND
            #             break
            #         else:
            #             raise e # 其他错误则抛出
            # cursor.close()

            return collections
        except Exception as e:
            logger.error(f"获取集合列表失败: {str(e)}")
            return []

    def read_collection(self, collection_name: str, limit: Optional[int] = None) -> List[Dict]:
        """
        读取指定集合的数据

        Args:
            collection_name: 集合名称 (可能需要是 "db_name.collection_name" 格式)
            limit: 限制返回的文档数量

        Returns:
            List[Dict]: 文档列表
        """
        if not self.session: # 检查 session 是否存在
            logger.error("未连接到数据库或会话未打开")
            return []

        try:
            # 通过 self.session 打开游标
            # 确保 collection_name 是 WiredTiger 期望的表 URI 格式，
            # 通常是 "table:your_db_name.your_collection_name"
            # 如果你的 collection_name 参数已经是这个格式，那很好。
            # 如果它只是一个简单的集合名，你可能需要预先处理它。
            table_uri = f"table:{collection_name}"
            cursor = self.session.open_cursor(table_uri, None, None)
            documents = []
            count = 0

            while True: # 使用 while True 和 try-except 来处理 cursor.next()
                try:
                    ret = cursor.next()
                    if ret != 0: # WT_NOTFOUND (обычно -26) or error
                        if ret == wiredtiger.WT_NOTFOUND: # 显式检查 WT_NOTFOUND
                            break
                        else:
                            # 抛出错误，让外部的 try-except 捕获
                            raise wiredtiger.WiredTigerError(f"cursor.next() failed with code: {ret}")

                    if limit and count >= limit:
                        break

                    key = cursor.get_key()
                    value = cursor.get_value() # 对于 MongoDB，value 通常是 BSON 编码的字节串
                    documents.append({
                        "key": key, # key 通常是 WiredTiger 的内部记录 ID 或索引键
                        "value": value # 这里是原始字节，可能需要 BSON 解码
                    })
                    count += 1
                except wiredtiger.WiredTigerError as e:
                    # 检查是否是 WT_NOTFOUND 错误，这表示没有更多数据了
                    if "WT_NOTFOUND" in str(e) or e.errno == wiredtiger.WT_NOTFOUND:
                        break
                    else:
                        logger.error(f"读取集合 {collection_name} 时游标迭代出错: {str(e)}")
                        raise e # 重新抛出，让外部的 try-except 捕获

            cursor.close()
            return documents
        except Exception as e:
            logger.error(f"读取集合 {collection_name} 失败: {str(e)}")
            return []

    def close(self):
        """关闭数据库连接和会话"""
        if self.session:
            self.session.close()
            self.session = None
            logger.info("WiredTiger 会话已关闭")
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("WiredTiger 连接已关闭")

def main():
    if len(sys.argv) < 2:
        print("使用方法: python wt_reader.py <数据目录路径> [集合名称] [限制数量]")
        sys.exit(1)

    data_dir = sys.argv[1]
    reader = WiredTigerReader(data_dir)

    if not reader.connect():
        sys.exit(1)

    try:
        # 列出所有集合
        collections = reader.list_collections()
        if not collections:
            print("未能获取到集合列表，或者没有集合。请检查日志和 MongoDB 的 WiredTiger 表结构。")
        else:
            print(f"发现 {len(collections)} 个集合 (表):")
            for coll in collections:
                print(f"- {coll}")

        # 如果指定了集合名称，则读取该集合的数据
        if len(sys.argv) > 2:
            collection_name_arg = sys.argv[2]
            # 假设用户提供的 collection_name_arg 就是 WiredTiger 中的表名
            # (例如 "myDB.myCollection")
            limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
            print(f"\n尝试读取集合 (表) '{collection_name_arg}' 的数据...")
            documents = reader.read_collection(collection_name_arg, limit)
            if documents:
                print(f"集合 {collection_name_arg} 中的前 {len(documents)} 条数据 (原始 WiredTiger 记录):")
                for doc in documents:
                    # 注意：这里的 value 是原始字节，如果是 MongoDB 数据，你需要 BSON 解码
                    # 例如：import bson; decoded_value = bson.decode(doc['value'])
                    print(f"  Key: {doc['key']}, Value (raw bytes length): {len(doc['value'])}")
                    # 为了演示，可以尝试打印少量字节
                    # print(f"    Value (first 50 bytes): {doc['value'][:50]}")
            else:
                print(f"未能从集合 (表) {collection_name_arg} 读取到数据，或集合为空。")
    finally:
        reader.close()

if __name__ == "__main__":
    main()
