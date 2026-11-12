"""An offline dlt source for the POC: two resources over local JSONL files.

`orders` is incremental on `id`. `customers` exists to show that the shim
loads the one stream named in the options and ignores the rest of the source.
`order-items` and `refunds_raw` write to tables with other names than the
resource. `nested_orders` writes two tables, which the shim refuses.
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

    # dlt writes this resource to the table `order_items` (it normalizes the name).
    @dlt.resource(name="order-items", write_disposition="append")
    def order_items():
        yield from _rows(os.path.join(data_dir, "order_items.jsonl"))

    # The table name differs from the resource name.
    @dlt.resource(name="refunds_raw", table_name="shop_refunds", write_disposition="append")
    def refunds_raw():
        yield from _rows(os.path.join(data_dir, "refunds.jsonl"))

    # Each row has a list in `items`. dlt writes the list to a child table,
    # `nested_orders__items`, so this resource writes two tables.
    @dlt.resource(name="nested_orders", write_disposition="append")
    def nested_orders():
        yield from _rows(os.path.join(data_dir, "nested_orders.jsonl"))

    return orders, customers, order_items, refunds_raw, nested_orders
