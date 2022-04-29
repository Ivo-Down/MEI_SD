Requires:
- Java 17+
- Maelstrom.jar with link-kv workload

Run:
```
java -jar maelstrom.jar test --bin sol.py -w lin-kv --time-limit 10 --node-count 10 --concurrency 20
java -Djava.awt.headless=true -jar maelstrom.jar test --bin lin-kv.py  -w lin-kv --time-limit 10 --concurrency 2n
java -Djava.awt.headless=true -jar Entrega_2/maelstrom.jar test --bin Entrega_2/raft.py  -w lin-kv --time-limit 50 --concurrency 2n --nemesis partition --log-stderr > lixo.txt
java -Djava.awt.headless=true -jar maelstrom.jar serve
```