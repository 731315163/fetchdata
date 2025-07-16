import io

import polars as pl
import pyarrow as pa



def serialize_dataframe(df: pl.DataFrame| pa.Table) -> bytes:
    """发送 Polars DataFrame 或 Arrow Table"""
    if isinstance(df, pl.DataFrame):
        # 使用 Polars 的 IPC 格式
        buf = io.BytesIO()
        df.write_ipc(buf)
        data = buf.getvalue()
        # 添加格式标识头
        return b"POLARS_IPC" + data
    elif isinstance(df, pa.Table):
        # 转换为 Arrow Stream 格式并序列化
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, df.schema) as writer:
            writer.write_table(df)
        data = sink.getvalue().to_pybytes()
        # 添加格式标识头
        return b"ARROW_IPC" + data
    else:
        raise ValueError("Unsupported serializer")

def deserialize_dataframe(data: bytes) -> pl.DataFrame:
    """
    从字节流反序列化为 Polars DataFrame
    """
    # 检查格式标识头
    if data.startswith(b"POLARS_IPC"):
        # 去除标识头后读取
        return pl.read_ipc(source=data[len(b"POLARS_IPC"):])
    elif data.startswith(b"ARROW_IPC"):
        # 去除标识头后读取
        reader = pa.ipc.open_stream(data[len(b"ARROW_IPC"):])
        pa_data = reader.read_all()
        df= pl.from_arrow(pa_data)
        if isinstance(df, pl.DataFrame):
            return df
        return df.to_frame()
    else:
        # 默认尝试作为 Polars IPC 读取
        return pl.read_ipc(source=data)
    


# ========================
# DataFrame 传输实现
# ========================
class DataFrameTransport:
  
    compression_level = 6  # 默认压缩级别

    @classmethod    
    def _compress(cls, data: bytes) -> bytes:
        """使用 LZ4 压缩算法"""
        return pa.compress(data, method='lz4', compression_level=cls.compression_level)
    @classmethod
    def _decompress(cls, data: bytes) -> bytes:
        """解压 LZ4 压缩数据"""
        return pa.decompress(data)
    @classmethod
    def serialize_dataframe(cls, df: pl.DataFrame|pa.Table, compress: bool = True):
        """
         DataFrame
        """
        # 1. 序列化
        raw_data = serialize_dataframe(df)
        
        # 2. 压缩（可选）
        if compress:
            raw_data = cls._compress(raw_data)
            
        # 3. 发送数据（使用 multipart 消息包含元数据）
        header = {
            'type': 'arrow',
            'compression': 'lz4' if compress else 'none',
            'size': len(raw_data),
            'schema': str(df.schema)
        }
        return header,raw_data
        
 
    
    def recv_dataframe(self, header,rawdata) -> pl.DataFrame:
        """
        通过 ZeroMQ 接收 DataFrame
        """
        # 1. 接收 header
        
        raw_data = rawdata
        
        # 2. 解压数据
        if header['compression'] == 'lz4':
            raw_data = self._decompress(raw_data)
            
        # 3. 反序列化
        return deserialize_dataframe(raw_data)