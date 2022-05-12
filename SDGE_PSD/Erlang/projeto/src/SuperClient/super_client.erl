-module(super_client).
-export([start/1]).
-define(EventList, [alarm, error, accident]).
-define(DevicesFileName, "dispositivos.json").


% Este módulo tem como objetivo criar dispositivos IOT e enviar eventos




start(Port) ->
  DevicesInfo = json_interpreter:parse_file(?DevicesFileName),
  create_devices(Port, DevicesInfo).


%manda pedido de autenticação ao coletor
device(Socket, DeviceInfo) ->  
  io:fwrite("\nSou um device, vou mandar auth info ~p.\n", maps:get(atom_to_binary(id),DeviceInfo)),
  maps:put(mode,auth,DeviceInfo),
  ok = gen_tcp:send(Socket, term_to_binary(DeviceInfo)),  %envia o evento ao coletor
  %TODO esperar pela confirmaçao da auth e só depois começar a enviar eventos
  receive
    {auth_ok} ->
      send_events(Socket, DeviceInfo);

    {auth_error} ->
      io:fwrite("\nDevice ~p failed authentication, shutting of.\n", maps:get(id,DeviceInfo))
  end.



% Cria X dispositivos e mete-os a mandar eventos para o coletor
create_devices(_,[]) ->
  ok;
create_devices(Port, [H|T]) ->
  {ok,Socket} = gen_tcp:connect("localhost", Port, [binary,{packet,4}]),  %cria uma nova ligaçao tcp ao coletor
  spawn(fun() -> device(Socket, H) end),
  create_devices(Port, T).




send_events(Socket, DeviceInfo) ->
  Event = lists:nth(rand:uniform(length(?EventList)), ?EventList),
  io:fwrite("\nSou um device, vou mandar event info ~p.\n", [Event]),
  EventInfo = #{dev_id=>maps:get(id,DeviceInfo), event_type=>Event, mode=>event},
  ok = gen_tcp:send(Socket, term_to_binary(EventInfo)),  %envia o evento ao coletor
  timer:sleep(1000),
  send_events(Socket, DeviceInfo).




