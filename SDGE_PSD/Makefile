help:
	@echo "use build-agg build-cli build-coll build-dev run-cli "

build: build-client build-aggregator

build-client:
	javac -cp lib/\* Constants/src/*.java Client/src/*.java -d bin

build-aggregator:
	javac -cp lib/\* Constants/src/*.java Agregador/DataStructs/*.java Agregador/*.java -d bin;
	cp Agregador/overlay.json bin/

build-collector:
	@echo TODO

run: run-bootstrap run-aggregators

run-collector:
	cd Erlang/projeto/ ; rebar3 shell ; $(shell 'coletor:start(1234,8301,"dispositivos.json"'))

run-bootstrap:
	cd bin/; java -cp .:../lib/* BootStrapper 8888 &

run-client:
	cd bin/; java -cp .:../lib/* Client ${zone}

run-aggregator:
	cd bin/; java -cp .:../lib/* AggregatorServer ${zone}

run-aggregators:
	cd bin/ ; \
	java -cp .:../lib/* AggregatorServer 1 & \
	java -cp .:../lib/* AggregatorServer 2 & \
	java -cp .:../lib/* AggregatorServer 3 & \
	java -cp .:../lib/* AggregatorServer 4 & \
	java -cp .:../lib/* AggregatorServer 5 & \

kill-all: kill-aggregators kill-bootstrap

kill-bootstrap:
	kill -9 $$(lsof -t -i:8888)

kill-aggregators: 
	kill -9 $$(lsof -t -i:8101) ; \
	kill -9 $$(lsof -t -i:8102) ; \
	kill -9 $$(lsof -t -i:8103) ; \
	kill -9 $$(lsof -t -i:8104) ; \
	kill -9 $$(lsof -t -i:8105) ; \
