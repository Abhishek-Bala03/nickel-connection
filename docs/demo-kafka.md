# Local Kafka access (PL-05 demo)

This starts a single-node Apache Kafka 4.1.2 broker with persistent storage.
The Connection stack must already be running (its Docker network is reused).
This does not switch the application's JSONL publisher to Kafka.

```sh
docker compose -p nickel-kafka-demo -f compose.kafka.yaml up -d --wait
docker compose -p nickel-kafka-demo -f compose.kafka.yaml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --if-not-exists --topic raw_data --partitions 1 --replication-factor 1
```

| Client | Bootstrap server |
| --- | --- |
| Connection service / containers on `nickel-connection_default` | `kafka:19092` |
| Host machine | `localhost:9092` |
| Nikhil's VM, after opening the tunnel below | `localhost:9092` |

Demo settings: topic `raw_data`, security protocol `PLAINTEXT`, no SASL
credentials, consumer group `nikhil-transform-demo`, `auto.offset.reset=earliest`.
Topic retention is 24 hours. One broker provides no replication redundancy.
Use synthetic demo records; this setup is not for real patient data.

## Nikhil's VM

Once the VM has SSH access to this machine, run on that VM and keep it open:

```sh
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:9092:127.0.0.1:9092 <user>@<connection-host>
```

The broker advertises `localhost:9092` on this listener, so both bootstrap and
subsequent Kafka connections pass through the tunnel. Do not publish port 9092
to the public network. A containerized remote consumer needs access to the
tunnel's network namespace (on Linux, `--network host` is one option).

From a Kafka installation on the VM, verify metadata and consume the dedicated
connectivity probe (not a patient envelope):

```sh
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw_data
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic nickel_demo_connectivity --from-beginning --max-messages 1 --timeout-ms 15000
```

Then point the Transform consumer at `raw_data`. Step 1's remote verification
remains pending until Nikhil reports a successful metadata request and probe
receipt. Contract agreement and application publishing are subsequent tasks.

Stop without removing messages:

```sh
docker compose -p nickel-kafka-demo -f compose.kafka.yaml stop
```

Based on the [official Apache Kafka Docker example](https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).
