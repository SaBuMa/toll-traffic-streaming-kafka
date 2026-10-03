# 🛠️ Troubleshooting

Issues you may hit while running this pipeline, and how to fix them.

### `NoBrokersAvailable`
The Kafka broker is not running (or not ready yet).
- Check the terminal running `kafka-server-start.sh`; it must stay open.
- Wait a few seconds after start-up before running the producer.

### `NotCoordinatorError` retries when the consumer starts
```
Marking the coordinator dead ... [Error 16] NotCoordinatorError
Attempt to join group toll-mysql-loader failed due to obsolete coordinator information
...
Successfully joined group toll-mysql-loader
```
**Harmless on a fresh broker.** The first time any consumer group connects, Kafka creates its
internal `__consumer_offsets` topic (where group bookmarks live). For about a second no broker
is "in charge" of the group yet, so the client retries until it is. It happened once in my run
and then joined successfully. The enhanced scripts set kafka-python's own logger to `WARNING`,
so you will only see this if it keeps failing.

### `DeprecationWarning: key_serializer does not implement kafka.serializer.Serializer`
kafka-python 3.x expects serializer *classes*, not plain functions such as `str.encode`.
The enhanced producer now encodes keys and values to bytes itself before `send()`,
which works on both 2.x and 3.x.

### Duplicate rows after running both versions
The course reader and the enhanced consumer read the **same topic**. The enhanced consumer
starts at offset 0 (`earliest`) with a brand-new group, so it re-loads events the course reader
already inserted. This is at-least-once delivery doing exactly what it promises: nothing lost,
but replays create copies. Check with query 6 in `sql/02_analysis_queries.sql`, and for a
clean demo empty the table first: `TRUNCATE TABLE livetolldata;`

### `Log directory ... is already formatted`
KRaft storage was formatted in an earlier run. Either skip the format step, or use
`--ignore-formatted` (already included in `scripts/setup_kafka.sh`).

### Consumer starts but loads nothing
- Topic name mismatch: the producer and consumer must both use `toll`.
- The course reader uses `auto_offset_reset='latest'` (the default), so messages produced
  **before** it started are skipped. Start the reader first, or use the enhanced consumer
  (`earliest`).
- The enhanced consumer uses a consumer group. If that group already read everything,
  it resumes from its bookmark. To replay from the start, use a new group:
  `KAFKA_GROUP_ID=replay-1 python3 src/enhanced/consumer.py`

### `Access denied for user 'root'`
The MySQL password in the Skills Network lab changes **every session**.
Copy the new one from the *Connection Information* tab into `.env`.

### Course reader: `NameError: name 'connection' is not defined`
A bug in the original `streaming-data-reader.py`: when the connection fails, the `except`
only prints a message and the script continues to `connection.cursor()`.
The enhanced consumer exits with a clear error instead.

### Timestamp parse errors
`time.ctime()` writes single-digit days with two spaces (`Fri Oct  2`). Python's `strptime`
treats a space in the format as "one or more spaces", so `'%a %b %d %H:%M:%S %Y'` handles it.
This case is covered by `tests/test_transform.py`.

### Everything is gone after reconnecting to the lab
The Skills Network environment is **not persistent**: Kafka, MySQL and your files are reset.
Re-run the setup steps (the scripts in `scripts/` make this quick).
