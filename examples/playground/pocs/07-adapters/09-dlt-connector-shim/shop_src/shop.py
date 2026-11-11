"""An offline dlt source for the POC: two resources over local JSONL files.

`orders` is incremental on `id`. `customers` exists to show that the shim
loads the one stream named in the options and ignores the rest of the source.
"""

import json
import os

import dlt

HERE = os.path.dirname(os.path.abspath(__file__))


def _rows(path):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


@dlt.source
def shop(data_dir: str = os.path.join(HERE, "data")):
    @dlt.resource(name="orders", write_disposition="append", primary_key="id")
    def orders(cursor=dlt.sources.incremental("id", initial_value=0)):
        for row in _rows(os.path.join(data_dir, "orders.jsonl")):
            if row["id"] > cursor.start_value:
                yield row

    @dlt.resource(name="customers", write_disposition="append")
    def customers():
        yield from _rows(os.path.join(data_dir, "customers.jsonl"))

    return orders, customers
