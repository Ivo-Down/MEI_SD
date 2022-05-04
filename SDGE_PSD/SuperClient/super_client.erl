-module(super_client).
-export([start/1]).

% Este módulo tem como objetivo criar dispositivos IOT e enviar pedidos





start(NumberOfDevices) ->
  Collector = spawn(fun() -> collector() end),
  create_devices(Collector, NumberOfDevices).


collector() ->
  receive
    {hello, From} ->
      io:fwrite("\nColetor recebeu hello de: ~p.\n", [From]),
      collector()
  end.
  



device(Collector) ->
  io:fwrite("\nSou um device, vou mandar msg.\n"),
  Collector ! hello,
  timer:sleep(1000),
  device(Collector).



% Cria X dispositivos e mete-os a mandar eventos para o coletor
create_devices(_,0) ->
  true;
create_devices(Collector, NumberOfDevices) ->
  spawn(fun() -> device(Collector) end),
  create_devices(Collector, NumberOfDevices - 1).






load_devices() ->
  {ok, File} = file:read_file("Ficheiros/dispositivos.json"),
  MyJSON = unicode:characters_to_list(File),
  jsx:decode(MyJSON, [return_maps]).


%rpc -> Remote Procedure Call
rpc(Req) ->
  ?MODULE ! {Req, self()}, 
  receive {Res, ?MODULE} -> Res end.

