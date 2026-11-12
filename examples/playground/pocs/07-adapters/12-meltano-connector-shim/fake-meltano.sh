#!/usr/bin/env bash
# Stand-in for `meltano`, used by the orders_name_mismatch connector only.
# `invoke tap-orders --discover` prints a catalog with the stream `orders`.
# The tap run then emits records under the name `Orders`, which does not
# match: the shim must fail instead of loading nothing.
if [ "$3" = "--discover" ]; then
    echo '{"streams":[{"tap_stream_id":"orders","stream":"orders","schema":{"type":"object"},"metadata":[]}]}'
else
    echo '{"type":"RECORD","stream":"Orders","record":{"id":1}}'
    echo '{"type":"STATE","value":{"bookmarks":{"orders":{"replication_key_value":1}}}}'
fi
