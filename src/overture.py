import os
import json
import pyarrow.parquet as pq
from overturemaps.core import record_batch_reader
from typing import List


def download_data():
    """
    Download data using overturemaps.

    Args:
        bbox (List[float]): Bounding box coordinates.
        output_folder (str): Folder to store the downloaded data (default: "./data/download").
    """

    bbox = [5.227521560265093, 51.66710614417271,
            5.385010872742034, 51.744275286954554]
    output_folder = "./data/download"
    datasets = [
        "segment",
        "place",
        "division_area",
        # "address",
        # "water"
    ]

    os.makedirs(output_folder, exist_ok=True)

    for dataset in datasets:
        print(f"Downloading {dataset} data...")
        __download(dataset, f"{dataset}.geoparquet", bbox, output_folder)


def __download(type: str, file, bbox: List[float], output_folder: str):
    reader = record_batch_reader(type, bbox)
    if reader is None:
        return

    output = os.path.join(output_folder, file)
    with __get_writer(output, reader.schema) as writer:
        __copy(reader, writer)


def __copy(reader, writer):
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows > 0:
            writer.write_batch(batch)


def __get_writer(path, schema):
    metadata = schema.metadata
    geo = json.loads(metadata[b"geo"])
    geo_columns = geo["columns"]
    if len(geo_columns) > 1:
        raise IOError("Expected single geom column but encountered multiple.")
    for geom_col_vals in geo_columns.values():
        if "bbox" in geom_col_vals:
            geom_col_vals.pop("bbox")
        if "bbox" in schema.names:
            geom_col_vals["covering"] = {
                "bbox": {
                    "xmin": ["bbox", "xmin"],
                    "ymin": ["bbox", "ymin"],
                    "xmax": ["bbox", "xmax"],
                    "ymax": ["bbox", "ymax"],
                }
            }
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    schema = schema.with_metadata(metadata)
    writer = pq.ParquetWriter(path, schema)

    return writer
