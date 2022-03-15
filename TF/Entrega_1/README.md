Requires:
- Java 17+
- Maelstrom.jar with link-kv workload

Run:
```
java -jar maelstrom.jar test --bin sol.py -w lin-kv --time-limit 10 --node-count 10 --concurrency 20
java -Djava.awt.headless=true -jar maelstrom.jar test --bin lin-kv.py  -w lin-kv --time-limit 10 --concurrency 2n
java -Djava.awt.headless=true -jar maelstrom.jar serve
```