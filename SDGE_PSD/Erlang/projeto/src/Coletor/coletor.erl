-module(coletor).
-export([start/1]).
-define(CollectTime, 5000).
-define(AliveTime, 60000).
-define(DevicesFileName, "dispositivos1.json").
-define(AggregatorPort, 8003).
-define(CollectorDeviceMsg, "C_Device").
-define(CollectorEventMsg, "C_Event").


start(Port) ->
  % Criar SocketListener para os dispositivos
  {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, 4}, {reuseaddr, true}]),

  % Criar zeromq socket para agregador (do tipo push)
  application:start(chumak),
  {ok, ChumakSocket} = chumak:socket(push),
  case chumak:connect(ChumakSocket, tcp, "localhost", ?AggregatorPort) of
    {ok, _BindPid} ->
        io:format("Binding OK with Pid: ~p\n", [ChumakSocket]);
    {error, Reason} ->
        io:format("Connection Failed for this reason: ~p\n", [Reason]);
    X ->
        io:format("Unhandled reply for bind ~p \n", [X])
  end,

  % Carregar os dados dos dispositivos para memória
  DevicesInfo = json_interpreter:parse_file(?DevicesFileName),

  spawn(fun() -> acceptor(LSock, ChumakSocket, DevicesInfo) end),
  started.
  


acceptor(LSock, ChumakSocket, DevicesInfo) ->
  % Fica à espera de uma nova conexão
  {ok, Sock} = gen_tcp:accept(LSock),
  io:fwrite("\nNew device connected to socket: ~p.\n", [Sock]),
  Pid = spawn(fun() -> handle_device(Sock, ChumakSocket, #{eventsList=>[], online=>true}, null, DevicesInfo) end),
  gen_tcp:controlling_process(Sock, Pid),
  acceptor(LSock, ChumakSocket, DevicesInfo).
  


% State = #{eventsList=>List, online=>Bool}, TRef é a referencia para o timer de inatividade
handle_device(Sock, ChumakSocket, State, TRef, DevicesInfo) ->
  receive
    {tcp, _, Data} ->
      inet:setopts(Sock, [{active, once}]),
      Msg = binary_to_term(Data),
      case maps:get(mode, Msg) of
        auth -> 
          io:fwrite("\nColetor recebeu auth info ~p .\n", [Msg]),
          login(maps:get(id, Msg), maps:get(password, Msg), binary_to_atom(maps:get(type, Msg)), DevicesInfo),
          timer:send_after(?CollectTime, aggregator), % começa aqui o timer para depois enviar info ao agregador
          handle_device(Sock, ChumakSocket, State, TRef, DevicesInfo);

        event -> 
          Event = maps:get(event_type, Msg),
          DeviceId = maps:get(id, Msg),
          io:fwrite("\nColetor recebeu evento: ~p do device ~p .\n", [Event, DeviceId]),

          timer:cancel(TRef),  % vai criar um novo timer para a questao da atividade
          {ok, NewTRef} = timer:send_after(?AliveTime, alive_timeout),   

          StateAux1 = maps:update(eventsList, maps:get(eventsList, State) ++ [Event], State),
          handle_device(Sock, ChumakSocket, StateAux1, NewTRef, DevicesInfo);

        _ -> io:fwrite("\nMensagem inválida!\n")
      end;

    {tcp_closed, _} ->
      io:fwrite("\nConnection closed.\n");

    {tcp_error, _, _} ->
      io:fwrite("\nConnection error.\n");

    {auth_ok, DeviceId, DeviceType} ->
      io:fwrite("\nAuthentication was successful.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_ok)),  %enviar para o device a confirmaçao de auth para ele começar a enviar eventos
      {ok, NewTRef} = timer:send_after(?AliveTime, alive_timeout),  % começar o timer para ver se está vivo 
      StateAux1 = maps:put(id, DeviceId, State),
      StateAux2 = maps:put(type, DeviceType, StateAux1),
      ok = sendState(ChumakSocket, StateAux2, ?CollectorDeviceMsg), % warns aggregator that device turned on
      handle_device(Sock, ChumakSocket, StateAux2, NewTRef, DevicesInfo);

    auth_error ->
      io:fwrite("\nFailed to authenticate.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_error)),
      gen_tcp:close(Sock);  % acaba a conexão com o dispositivo

    alive_timeout ->
      io:fwrite("\nDevice has not sent anything in a while. Turning it to offline.\n"),
      ok = sendState(ChumakSocket, maps:update(online, false, State), ?CollectorDeviceMsg), % warns aggregator that device turned off
      handle_device(Sock, ChumakSocket, State, TRef, DevicesInfo);

    aggregator ->
      io:fwrite("\nSending info to aggregator: ~p\n",[State]),
      % Envia estado para o agregador através de um push zeromq socket
      ok = sendState(ChumakSocket, State, ?CollectorEventMsg),
      handle_device(Sock, ChumakSocket, maps:update(eventsList, [], State), TRef, DevicesInfo)
  end.






login(DeviceId, DevicePw, DeviceType, DevicesInfo) ->
  DeviceDictionary = lists:nth(DeviceId, DevicesInfo),
  case maps:find(id, DeviceDictionary) of

    {ok, DeviceId} ->
      case maps:find(password, DeviceDictionary) of
        {ok, DevicePw} ->
          io:fwrite("\nAuth successful.\n"),
          self() ! {auth_ok, DeviceId, DeviceType};
        {ok, _} ->
          io:fwrite("\nAuth failed.\n"),
          self() ! auth_error      
      end;
      
    {ok, _} ->
      io:fwrite("\nAuth failed.\n"),
      self() ! auth_error  
  end.


% Sends device's state to aggregator
sendState(ChumakSocket, State, SendType) ->  %SendType: C_Event or _CDevice
  ToSend = [list_to_binary(SendType), term_to_binary(State)],
  ok = chumak:send_multipart(ChumakSocket, ToSend),
  timer:send_after(?CollectTime, aggregator),
  ok.