import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def make_parquet_from_csv(csv_file_path: str, parquet_file_path: str,) -> None:
    """
    Docstring for make_parquet_from_csv
    
    :param csv_file_path: Description
    :type csv_file_path: str
    :param parquet_file_path: Description
    :type parquet_file_path: str
    """   
    writer = None
    chunks = pd.read_csv(csv_file_path, chunksize=1_000_000,dtype=str,low_memory=False)
    for chunk in chunks:
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(parquet_file_path, table.schema, compression="snappy")
        writer.write_table(table)

    writer.close()