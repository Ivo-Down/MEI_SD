-module(coletor).
-export([start/2, start/3]).
-import(login_manager,[start/1, login/2]).
-define(CollectTime, 15000).
-define(AliveTime, 60000).
-define(DevicesFileName, "dispositivos.json").
-define(CollectorDeviceMsg, "C_Device").
-define(CollectorEventMsg, "C_Event").

start(Port,AggregatorPort) ->
  io:fwrite("\nDevices file: ~p\n",[?DevicesFileName]),

  % Criar SocketListener para os dispositivos
  {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, 4}, {reuseaddr, true}, {backlog, 1000}]),

  % Criar zeromq socket para agregador (do tipo push)
  application:start(chumak),
  {ok, ChumakSocket} = chumak:socket(push),

  case chumak:connect(ChumakSocket, tcp, "localhost", AggregatorPort) of
    {ok, _BindPid} ->
        io:format("Binding OK with Pid: ~p\n", [ChumakSocket]);
    {error, Reason} ->
        io:format("Connection Failed for this reason: ~p\n", [Reason]);
    X ->
        io:format("Unhandled reply for bind ~p \n", [X])
  end,

  spawn(fun() -> acceptor(LSock, ChumakSocket) end),
  started.
  

start(Port,AggregatorPort, JsonFile) ->
  io:fwrite("\nDevices file: ~p\n",[JsonFile]),

  % Criar SocketListener para os dispositivos
  {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, 4}, {reuseaddr, true}, {backlog, 1000}]),

  % Criar zeromq socket para agregador (do tipo push)
  application:start(chumak),
  {ok, ChumakSocket} = chumak:socket(push),

  case chumak:connect(ChumakSocket, tcp, "localhost", AggregatorPort) of
    {ok, _BindPid} ->
        io:format("Binding OK with Pid: ~p\n", [ChumakSocket]);
    {error, Reason} ->
        io:format("Connection Failed for this reason: ~p\n", [Reason]);
    X ->
        io:format("Unhandled reply for bind ~p \n", [X])
  end,

  spawn(fun() -> acceptor(LSock, ChumakSocket) end),
  started.


acceptor(LSock, ChumakSocket) ->
  % Fica à espera de uma nova conexão
  {ok, Sock} = gen_tcp:accept(LSock),
  %io:fwrite("\nNew device connected to socket: ~p.\n", [Sock]),
  Pid = spawn(fun() -> handle_device(Sock, ChumakSocket, #{eventsList=>[], online=>true}, null) end),
  gen_tcp:controlling_process(Sock, Pid),
  acceptor(LSock, ChumakSocket).
  


% State = #{eventsList=>List, online=>Bool}, TRef é a referencia para o timer de inatividade
handle_device(Sock, ChumakSocket, State, TRef) ->
  receive
    {tcp, _, Data} ->
      inet:setopts(Sock, [{active, once}]),
      Msg = binary_to_term(Data),
      case maps:get(mode, Msg) of
        auth -> 
          login_manager:login(maps:get(id, Msg), maps:get(password, Msg), erlang:binary_to_atom(maps:get(type, Msg)), self()),
          timer:send_after(random_interval(?CollectTime), aggregator), % começa aqui o timer para depois enviar info ao agregador
          handle_device(Sock, ChumakSocket, State, TRef);

        event -> 
          Event = maps:get(event_type, Msg),

          timer:cancel(TRef),  % vai criar um novo timer para inatividade
          {ok, NewTRef} = timer:send_after(?AliveTime, alive_timeout),   

          StateAux1 = maps:update(eventsList, maps:get(eventsList, State) ++ [Event], State),
          handle_device(Sock, ChumakSocket, StateAux1, NewTRef);

        _ -> io:fwrite("\nInvalid Message!\n")
      end;

    {tcp_closed, _} ->
      NewMap = maps:update(online, false, State),
      ok = sendState(ChumakSocket, NewMap, ?CollectorDeviceMsg),
      io:fwrite("\nConnection closed.\n");

    {tcp_error, _, _} ->
      ok = sendState(ChumakSocket, maps:update(online, false, State), ?CollectorDeviceMsg),
      io:fwrite("\nConnection error.\n");

    {auth_ok, DeviceId, DeviceType} ->
      ok = gen_tcp:send(Sock, erlang:atom_to_binary(auth_ok)),  %enviar para o device a confirmaçao de auth para ele começar a enviar eventos
      {ok, NewTRef} = timer:send_after(?AliveTime, alive_timeout),  % começar o timer para ver se está vivo 
      StateAux1 = maps:put(id, DeviceId, State),
      StateAux2 = maps:put(type, DeviceType, StateAux1),
      ok = sendState(ChumakSocket, StateAux2, ?CollectorDeviceMsg), % warns aggregator that device turned on
      handle_device(Sock, ChumakSocket, StateAux2, NewTRef);

    auth_error ->
      ok = gen_tcp:send(Sock, erlang:atom_to_binary(auth_error)),
      gen_tcp:close(Sock);  % acaba a conexão com o dispositivo

    alive_timeout ->
      io:fwrite("\nDevice has not sent anything in a while. Turning it to offline.\n"),
      ok = sendState(ChumakSocket, maps:update(online, false, State), ?CollectorDeviceMsg), % warns aggregator that device turned off
      handle_device(Sock, ChumakSocket, State, TRef);

    aggregator ->
      % Envia estado para o agregador através de um push zeromq socket
      ok = sendState(ChumakSocket, State, ?CollectorEventMsg),
      handle_device(Sock, ChumakSocket, maps:update(eventsList, [], State), TRef)
  end.


% Sends device's state to aggregator
sendState(ChumakSocket, State, SendType) ->  %SendType: C_Event or _CDevice
  case SendType of
    ?CollectorDeviceMsg -> ToSend = [list_to_binary(SendType), term_to_binary(State)],
                          ok = chumak:send_multipart(ChumakSocket, ToSend),
                          timer:send_after(random_interval(?CollectTime), aggregator),
                          ok;
    ?CollectorEventMsg ->
      case length(maps:get(eventsList, State)) of 
        0 -> timer:send_after(random_interval(?CollectTime), aggregator), ok; %if there are no events to send, don't send
        _ -> ToSend = [list_to_binary(SendType), term_to_binary(State)],
            ok = chumak:send_multipart(ChumakSocket, ToSend),
            timer:send_after(random_interval(?CollectTime), aggregator),
            ok
      end
    end.
    

random_interval(Base) -> 
  Base + round(((rand:uniform() * 8)-3)*1000).