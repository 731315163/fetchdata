import logging
from datetime import datetime, timedelta, timezone
from typing import MutableSequence, Sequence

import polars as pl
from polars import DataFrame, LazyFrame

from tradepulse. data.protocol import DataKey, DataRecoder
from tradepulse. typenums import DataType, MarketType

logger = logging.getLogger(__name__)

class DataFrameRecoder(DataRecoder[DataFrame]):
    
    def __init__(self, pair: str, marketType: MarketType, datatype: DataType, timeframe="", data: DataFrame | None = None, timeout: timedelta = timedelta(minutes=10)):
        self.data= DataFrame() if data is None else data

        super().__init__(pair=pair, marketType=marketType, datatype=datatype, timeframe=timeframe,timeout_ms=timeout)
        self.timekey = "timestamp"

    @staticmethod
    def Empty() -> DataFrame:
        return DataFrame()
   
    @property
    def rawdata(self) -> DataFrame:
        return self.data

    @property
    def is_empty(self) -> bool:
        return self.data.is_empty()
     

    @property
    def empty(self)->DataFrame:
         return DataFrame()
    def append(self, data: DataFrame, dt: datetime | int | float | None = None):
     

        # 如果提供了时间，则添加或更新时间列
        # if dt is not None:
           
        #     # if self.timekey in data.columns:
        #     #     data = data.with_columns(pl.lit(dt_normalized).alias(self.timekey))
           
        # else:
            # 确保数据包含有效的时间列
        if self.timekey not in data.columns:
                raise ValueError(f"未提供时间参数且数据缺少时间列 '{self.timekey}'")

        # 确保时间列格式正确
        if not self.data.is_empty():
            data = self._ensure_time_format(data)

        
        self.data = pl.concat([self.data, data])
  
     

    def prepend(self, data: DataFrame, dt: datetime | int | float | None = None):
        """向数据记录器前置数据"""
      

        # if dt is not None:
        #     dt_normalized = self.normalize_datetime(dt)
        #     if self.timekey in data.columns:
        #         data = data.with_columns(pl.lit(dt_normalized).alias(self.timekey))
          
        # else:
        #     if self.timekey not in data.columns:
        #         raise ValueError(f"未提供时间参数且数据缺少时间列 '{self.timekey}'")

        # if not self.data.is_empty():
        #     data = self._ensure_time_format(data)

        if self.data.is_empty():
            self.data = data
        else:
            self.data = pl.concat([data, self.data])
       
            



    def _ensure_time_format(self, data: DataFrame) -> DataFrame:
        """确保新数据的时间列格式与现有数据一致"""
        if self.timekey not in data.columns or self.timekey not in self.data.columns:
            return data

        existing_dtype = self.data[self.timekey].dtype
        new_dtype = data[self.timekey].dtype

        if existing_dtype != new_dtype:
            # 尝试转换为现有类型
            try:
                if existing_dtype == pl.Datetime:
                    return data.with_columns(pl.col(self.timekey).cast(pl.Datetime(time_zone="UTC")))
                elif existing_dtype == pl.Int64:
                    return data.with_columns(pl.col(self.timekey).cast(pl.Int64))
            except Exception as e:
                raise ValueError(f"无法将时间列从 {new_dtype} 转换为 {existing_dtype}: {e}")

        return data

    def prune_expired_data(self,td:timedelta|int|None = None):
        """删除超时的数据"""
        if self.data.is_empty() or not hasattr(self, 'timekey') or not self.timekey or self.timekey not in self.data.columns:
            return

        # 计算截止时间
        cutoff = datetime.now(timezone.utc) - self.timeout
        try:
            # 获取时间列表达式
            time_expr = pl.col(self.timekey)

            # 如果是整数时间戳，转换为datetime
            if self.data[self.timekey].dtype in (pl.Int64, pl.Float64):
                # 假设为毫秒级时间戳
                time_expr = time_expr.cast(pl.Datetime(time_unit='ms', time_zone="UTC"))
            # 过滤数据
            self.data = self.data.filter(time_expr >= cutoff.timestamp()*1000)
        except Exception as e:
            # 记录错误但继续运行
            logger.error(f"警告: 修剪过期数据时出错: {e}")

    def __getitem__(self, index)->DataFrame:
        """支持索引访问和切片"""
        lazy_df = self.data.lazy()  # 转换为惰性Frame
        start = None
        end =None
        if isinstance(index, slice):
        # 处理切片：假设start/end为时间戳
            start,end = index.start ,index.stop
        elif isinstance(index, (int,float)) :
            start = index
        elif isinstance(index, (tuple,Sequence,MutableSequence)) and len(index)>0:
            start = index[0]
            if len(index) >= 2:
                end = index[1]
                
            
        if start is not None:
            lazy_df = lazy_df.filter(pl.col(self.timekey) >= start)
        if end is not None:
            lazy_df = lazy_df.filter(pl.col(self.timekey) <= end)
        df = lazy_df.collect()  # 一次性触发执行
        return df
    def __len__(self) -> int:
        return self.data.height