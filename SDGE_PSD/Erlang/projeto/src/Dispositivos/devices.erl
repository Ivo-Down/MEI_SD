-module(devices).
-export([start/1]).
-define(EventList, [alarm, error, accident]).
-define(DevicesFileName, "dispositivos1.json").
-define(EventTime, 1000).


% Este módulo tem como objetivo criar dispositivos IOT e enviar eventos


start(Port) ->
  DevicesInfo = json_interpreter:parse_file(?DevicesFileName),
  create_devices(Port, DevicesInfo).


%manda pedido de autenticação ao coletor
device(Socket, DeviceInfo) ->  
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
          send_events(Socket, EventDeviceInfo);

        auth_error ->
          io:fwrite("\nDevice ~p failed authentication, shutting of.\n", maps:get(id,AuthDeviceInfo))
      end;

    {error, Reason}->
      io:fwrite("\nAn error has occurred:  ~p.\n", [Reason])
  end.




% Cria X dispositivos e mete-os a mandar eventos para o coletor
create_devices(_,[]) ->
  ok;
create_devices(Port, [H|T]) ->
  {ok,Socket} = gen_tcp:connect("localhost", Port, [binary,{packet,4}, {active, false}]),  %cria uma nova ligaçao tcp ao coletor
  spawn(fun() -> device(Socket, H) end),
  create_devices(Port, T).



send_events(Socket, DeviceInfo) ->
  Event = lists:nth(rand:uniform(length(?EventList)), ?EventList),
  io:fwrite("\nSou um device, vou mandar event info ~p.\n", [Event]),
  EventInfo = #{id=>maps:get(id,DeviceInfo), event_type=>Event, mode=>event},
  ok = gen_tcp:send(Socket, term_to_binary(EventInfo)),  %envia o evento ao coletor
  timer:sleep(?EventTime),
  send_events(Socket, DeviceInfo).




