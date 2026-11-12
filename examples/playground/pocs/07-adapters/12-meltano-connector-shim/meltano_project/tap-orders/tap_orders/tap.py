"""A tiny offline Singer tap, built with the Meltano Singer SDK.

It stands in for any real tap in the POC: it reads two local JSONL files and
needs no network. `orders` is incremental on `id` (the SDK writes a bookmark
into the STATE message). `customers` is a full-table stream.
"""

import json
import os

from singer_sdk import Stream, Tap
from singer_sdk import typing as th


class _JsonlStream(Stream):
    filename = ""

    def get_records(self, context):
        path = os.path.join(self.config["data_dir"], self.filename)
        if not os.path.exists(path):
            return
        start = self.get_starting_replication_key_value(context)
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                if start is not None and row["id"] <= start:
                    continue
                yield row


class OrdersStream(_JsonlStream):
    name = "orders"
    filename = "orders.jsonl"
    primary_keys = ["id"]
    replication_key = "id"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("amount_cents", th.IntegerType),
    ).to_dict()


class CustomersStream(_JsonlStream):
    name = "customers"
    filename = "customers.jsonl"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("name", th.StringType),
    ).to_dict()


class TapOrders(Tap):
    name = "tap-orders"
    config_jsonschema = th.PropertiesList(
        th.Property("data_dir", th.StringType, required=True),
    ).to_dict()

    def discover_streams(self):
        return [OrdersStream(self), CustomersStream(self)]


if __name__ == "__main__":
    TapOrders.cli()
