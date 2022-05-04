-module(super_client).
-export([start/2]).
-define(EventList, [alarm, error, accident]).

% Este módulo tem como objetivo criar dispositivos IOT e enviar eventos



start(Port, NumberOfDevices) ->
  %Collector = spawn(fun() -> collector() end),
  create_devices(Port, NumberOfDevices).




device(Socket) ->
  %TODO importar auth info do json
  ID = 69,
  Type = "fridge",
  Password = "pw123",
  io:fwrite("\nSou um device, vou mandar auth info ~p.\n", [ID]),
  %Msg = messages:encode_msg(#{id=> ID, type=>Type, password=>Password}, "auth_info"),   TODO descobrir de onde vem este messages
  Msg = lists:nth(rand:uniform(length(?EventList)), ?EventList),
  ok = gen_tcp:send(Socket, atom_to_binary(Msg)),  %envia o evento ao coletor
  %TODO esperar pela confirmaçao da auth e só depois começar a enviar eventos
  send_events(Socket).



% Cria X dispositivos e mete-os a mandar eventos para o coletor
create_devices(_,0) ->
  true;
create_devices(Port, NumberOfDevices) ->
  {ok,Socket} = gen_tcp:connect("localhost", Port, [binary,{packet,4}]),  %cria uma nova ligaçao tcp ao coletor
  spawn(fun() -> device(Socket) end),
  create_devices(Port, NumberOfDevices - 1).




send_events(Socket) ->
  Event = lists:nth(rand:uniform(length(?EventList)), ?EventList),
  io:fwrite("\nSou um device, vou mandar auth info ~p.\n", [Event]),
  ok = gen_tcp:send(Socket, atom_to_binary(Event)),  %envia o evento ao coletor
  timer:sleep(1000),
  send_events(Socket).











load_devices() ->
  {ok, File} = file:read_file("Ficheiros/dispositivos.json"),
  MyJSON = unicode:characters_to_list(File),
  jsx:decode(MyJSON, [return_maps]).


%rpc -> Remote Procedure Call
rpc(Req) ->
  ?MODULE ! {Req, self()}, 
  receive {Res, ?MODULE} -> Res end.

