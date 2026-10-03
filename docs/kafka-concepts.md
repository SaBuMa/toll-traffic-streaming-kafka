# 📬 Kafka Concepts — The Post Office Analogy

A quick mental model of the Kafka pieces used in this project.

## The analogy

Imagine every toll plaza has a clerk who writes a **postcard** each time a vehicle passes.
Kafka is the **post office** that stores those postcards in order, and the MySQL loader
is a **filing clerk** who picks them up and files them in a cabinet (the database).

| Kafka term | Post office | In this project |
|---|---|---|
| **Producer** | Clerk writing and dropping postcards | `producer.py` / `toll_traffic_generator.py` |
| **Message** | One postcard | `"Fri Oct  2 19:26:20 2026,9820660,truck,4004"` |
| **Broker** | The post office building | Kafka server on `localhost:9092` |
| **Topic** | A named PO box | `toll` |
| **Partition** | A slot inside the PO box | 1 by default (`PARTITIONS=3` to try more) |
| **Offset** | The postcard's number in its slot | 0, 1, 2, 3 ... |
| **Key** | The address written on the postcard | Plaza id, e.g. `4004` |
| **Consumer group** | The filing team | `toll-mysql-loader` |
| **Committed offset** | The team's bookmark: "filed up to #57" | Saved after each DB commit |
| **KRaft** | The post office's own management office (no outside manager) | Replaces ZooKeeper |

## The flow

```
  PRODUCER                    BROKER (topic: toll)                 CONSUMER            DATABASE
 ┌──────────┐   send()    ┌─────────────────────────────┐   poll()  ┌──────────┐  INSERT ┌──────────────┐
 │ toll     │ ──────────▶ │ partition 0: [0][1][2][3]...│ ────────▶ │ parse +  │ ──────▶ │ livetolldata │
 │ plazas   │             └─────────────────────────────┘           │ validate │         └──────────────┘
 └──────────┘                              ▲                        └────┬─────┘
                                           │        commit offset        │
                                           └─────────────────────────────┘
                                             (only AFTER the DB commit)
```

## Why the key matters

Kafka only guarantees order **inside one partition**. Using the plaza id as the key means
the post office always puts postcards from plaza 4004 in the same slot:

```
 key=4004 ──▶ hash ──▶ partition 1 ──▶ 4004's events stay in order
 key=4007 ──▶ hash ──▶ partition 0
 key=4001 ──▶ hash ──▶ partition 2
```

With the lab's single partition everything is ordered anyway; the key starts paying off
as soon as the topic gets more partitions and more consumers in the group.

## Why "commit after the database"

The bookmark (committed offset) decides where the team resumes after a crash.

```
 Safe order (enhanced consumer):           Risky order:
 1. insert rows into MySQL                 1. move bookmark
 2. MySQL COMMIT                           2. crash here 💥
 3. move bookmark                          → those postcards are never filed

 Crash between 2 and 3 → a few rows are re-filed (duplicates, but nothing lost)
```

This is called **at-least-once delivery**.

## `earliest` vs `latest`

When a consumer group has no bookmark yet:

- `latest` (kafka-python default, used by the course reader): start with **new** postcards only.
  Anything sent before the reader started is skipped. That is why the lab asks you to keep
  the reader running while the generator streams.
- `earliest` (enhanced consumer): start from postcard #0, so nothing already in the box is missed.
