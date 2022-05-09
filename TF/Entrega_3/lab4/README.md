Requires:
- Java 17+
- Maelstrom.jar with link-kv workload

Run:
```
java -Djava.awt.headless=true -jar maelstrom.jar test --bin cent.py  -w txn-list-append --node-count 1

java -Djava.awt.headless=true -jar maelstrom.jar test --bin cent.py  -w txn-list-append --node-count 5 --latency 100 --time-limit 10

java -Djava.awt.headless=true -jar maelstrom.jar test --bin cent.py  -w txn-list-append --node-count 5 --latency 100 --time-limit 20 --log-stderr

java -Djava.awt.headless=true -jar maelstrom.jar serve
```