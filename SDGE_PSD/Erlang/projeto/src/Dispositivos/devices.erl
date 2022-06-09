-module(devices).
-export([start/1]).
-define(EventList, [alarm, error, accident]).
-define(DevicesFileName, "dispositivos_1000.json").
-define(EventTime, 1000).
-define(ChangeZoneTimer, 5000).



% Este módulo tem como objetivo criar dispositivos IOT e enviar eventos a uns dos coletores


start(PortList) ->
  DevicesInfo = json_interpreter:parse_file(?DevicesFileName),
  create_devices(PortList, DevicesInfo).



% Cria X dispositivos e mete-os a mandar eventos para o coletor
create_devices(_,[]) ->
  ok;
create_devices(PortList, [H|T]) ->
  Collector = choose_colector(PortList),
  {ok,Socket} = gen_tcp:connect("localhost", Collector, [binary,{packet,4}, {active, false}]),  %cria uma nova ligaçao tcp ao coletor
  DevicePid = spawn(fun() -> device_loop1(Socket, H, PortList, no_auth, Collector) end),
  timer:send_after(?ChangeZoneTimer, DevicePid, change_zone),  % envia o 1º pedido de mudar zona passado x tempo
  create_devices(PortList, T).



% Os devices vão estar a alternar entre 2 loops de enviar eventos, mudar de zonas e fazer autenticações
device_loop1(Socket, DeviceInfo, PortList, AuthState, ActualCol) ->
  if 
    AuthState == no_auth ->
      {ok, NewDeviceInfo} = device_auth(Socket, DeviceInfo),
      device_loop2(Socket, NewDeviceInfo, PortList, auth, ActualCol);

    AuthState == auth ->      
      send_event(Socket, DeviceInfo),
      timer:sleep(?EventTime),
      device_loop2(Socket, DeviceInfo, PortList, auth, ActualCol)
  end.


% Este loop serve para verificar se já é altura de mudar de zona
device_loop2(Socket, DeviceInfo, PortList, AuthState, ActualCol) ->
  receive
    change_zone ->
      NewCol = choose_colector(PortList),  % escolhe um novo coletor
      timer:send_after(?ChangeZoneTimer, change_zone), %cria novo timer
      c:flush(), % limpa a queue de mensagens, penso ser desnecessário

      if
        NewCol /= ActualCol ->
          io:fwrite("\nChanging zone to: ~p\n",[NewCol]),
          ok = gen_tcp:shutdown(Socket, read),  % fecha a ligação com o coletor antigo
          {ok, NewSocket} = gen_tcp:connect("localhost", NewCol, [binary,{packet,4}, {active, false}]),
          device_loop1(NewSocket, DeviceInfo, PortList, no_auth, NewCol);

        true ->
          io:fwrite("\nStaying in the same collector: ~p\n",[ActualCol]),
          device_loop1(Socket, DeviceInfo, PortList, AuthState, ActualCol)
      end

    after 0 ->
      device_loop1(Socket, DeviceInfo, PortList, AuthState, ActualCol)
  end.



%manda pedido de autenticação ao coletor
device_auth(Socket, DeviceInfo) ->  
  DeviceId = maps:get(id,DeviceInfo),
  io:fwrite("\nSou o device ~p, vou mandar auth info.\n", [DeviceId]),
  AuthDeviceInfo = maps:put(mode, auth, DeviceInfo),
  ok = gen_tcp:send(Socket, term_to_binary(AuthDeviceInfo)),  %envia pedido de autenticação ao coletor
  % Espera pela resposta da autenticação, é uma espera bloqueante
  case gen_tcp:recv(Socket, 0) of
    {ok, Binary}->% Send basic message.
      Msg = binary_to_atom(Binary),
      case Msg of
            
        auth_ok ->   
          io:fwrite("\nDevice ~p authenticated!\n", [maps:get(id,AuthDeviceInfo)]),
          EventDeviceInfo = maps:put(mode, event, DeviceInfo),
          {ok, EventDeviceInfo};
          %send_events(Socket, EventDeviceInfo);

        auth_error ->
          io:fwrite("\nDevice ~p failed authentication, shutting of.\n", maps:get(id,AuthDeviceInfo))
      end;

    {error, Reason}->
      io:fwrite("\nAn error has occurred:  ~p.\n", [Reason])
  end.



% Envia um evento ao coletor
send_event(Socket, DeviceInfo) ->
  Event = lists:nth(rand:uniform(length(?EventList)), ?EventList),
  io:fwrite("\nSou o device ~p, vou mandar event info ~p.\n", [DeviceInfo, Event]),
  EventInfo = #{id=>maps:get(id,DeviceInfo), event_type=>Event, mode=>event},
  ok = gen_tcp:send(Socket, term_to_binary(EventInfo)).  %envia o evento ao coletor



choose_colector(ColList) ->
  Collector = lists:nth(rand:uniform(length(ColList)), ColList),
  Collector.

