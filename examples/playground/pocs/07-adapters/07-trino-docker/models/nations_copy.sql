-- Copy the 25-row tpch.tiny.nation table into a new memory.rocky_demo
-- table via Rocky's full_refresh CTAS path. The Trino dialect emits
-- `CREATE TABLE "memory"."rocky_demo"."nations_copy" AS <select>`.
SELECT
    nationkey,
    name,
    regionkey,
    comment
FROM tpch.tiny.nation
ORDER BY nationkey
