-module(super_client).
-export([start/1]).
-define(EventList, [alarm, error, accident]).
-define(DevicesFileName, "dispositivos.json").


% Este módulo tem como objetivo criar dispositivos IOT e enviar eventos




start(Port) ->
  {ok, DevicesInfo} = json_interpreter:parse_file(?DevicesFileName),
  create_devices(Port, maps:iterator(DevicesInfo)).



device(Socket) ->
  %TODO importar auth info do json
  {ok, DevicesInfo} = json_interpreter:parse_file(?DevicesFileName),
  ID = 69,
  Type = "fridge",
  Password = "pw123",
  io:fwrite("\nSou um device, vou mandar auth info ~p.\n", [ID]),
  %Msg = messages:encode_msg(#{id=> ID, type=>Type, password=>Password}, "auth_info"),   TODO descobrir de onde vem este messages
  DeviceInfo = #{dev_id=>ID, dev_type=>Type, dev_password=>Password, mode=>auth},
  ok = gen_tcp:send(Socket, term_to_binary(DeviceInfo)),  %envia o evento ao coletor
  %TODO esperar pela confirmaçao da auth e só depois começar a enviar eventos
  receive
    {auth_ok} ->
      send_events(Socket);

    {auth_error} ->
      io:fwrite("\nDevice ~p ailed authentication, shutting of.\n", [ID])
  end.



% Cria X dispositivos e mete-os a mandar eventos para o coletor
create_devices(_,none) ->
  ok;
create_devices(Port, DevicesInfoIterator) ->
  {Key, Value, NextIterator} = maps:next(DevicesInfoIterator),  %TODO FIQUEI AQUI
  {ok,Socket} = gen_tcp:connect("localhost", Port, [binary,{packet,4}]),  %cria uma nova ligaçao tcp ao coletor
  spawn(fun() -> device(Socket) end),
  create_devices(Port, maps:next(NextIterator)).




send_events(Socket) ->
  Event = lists:nth(rand:uniform(length(?EventList)), ?EventList),
  io:fwrite("\nSou um device, vou mandar event info ~p.\n", [Event]),
  EventInfo = #{dev_id=>1, event_type=>Event, mode=>event},
  ok = gen_tcp:send(Socket, term_to_binary(EventInfo)),  %envia o evento ao coletor
  timer:sleep(1000),
  send_events(Socket).




