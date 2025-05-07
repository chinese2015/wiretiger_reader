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
        
    def connect(self) -> bool:
        """
        连接到WiredTiger数据库
        
        Returns:
            bool: 连接是否成功
        """
        try:
            self.conn = wiredtiger.wiredtiger_open(
                self.data_dir,
                "create=false,readonly=true"
            )
            logger.info("成功连接到WiredTiger数据库")
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
        if not self.conn:
            logger.error("未连接到数据库")
            return []
            
        try:
            cursor = self.conn.open_cursor("table:collection", None, None)
            collections = []
            while cursor.next() == 0:
                collections.append(cursor.get_key())
            cursor.close()
            return collections
        except Exception as e:
            logger.error(f"获取集合列表失败: {str(e)}")
            return []
            
    def read_collection(self, collection_name: str, limit: Optional[int] = None) -> List[Dict]:
        """
        读取指定集合的数据
        
        Args:
            collection_name: 集合名称
            limit: 限制返回的文档数量
            
        Returns:
            List[Dict]: 文档列表
        """
        if not self.conn:
            logger.error("未连接到数据库")
            return []
            
        try:
            cursor = self.conn.open_cursor(f"table:{collection_name}", None, None)
            documents = []
            count = 0
            
            while cursor.next() == 0:
                if limit and count >= limit:
                    break
                    
                key = cursor.get_key()
                value = cursor.get_value()
                documents.append({
                    "key": key,
                    "value": value
                })
                count += 1
                
            cursor.close()
            return documents
        except Exception as e:
            logger.error(f"读取集合 {collection_name} 失败: {str(e)}")
            return []
            
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None

def main():
    if len(sys.argv) < 2:
        print("使用方法: python wt_reader.py <数据目录路径>")
        sys.exit(1)
        
    data_dir = sys.argv[1]
    reader = WiredTigerReader(data_dir)
    
    if not reader.connect():
        sys.exit(1)
        
    try:
        # 列出所有集合
        collections = reader.list_collections()
        print(f"发现 {len(collections)} 个集合:")
        for coll in collections:
            print(f"- {coll}")
            
        # 如果指定了集合名称，则读取该集合的数据
        if len(sys.argv) > 2:
            collection_name = sys.argv[2]
            limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
            documents = reader.read_collection(collection_name, limit)
            print(f"\n集合 {collection_name} 中的数据:")
            for doc in documents:
                print(doc)
    finally:
        reader.close()

if __name__ == "__main__":
    main() 
