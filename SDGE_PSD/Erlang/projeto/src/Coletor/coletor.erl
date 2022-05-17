-module(coletor).
-export([start/1]).
-define(CollectTime, 10000).
-define(AliveTime, 20000).
-define(DevicesFileName, "dispositivos.json").


start(Port) ->
  {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, 4}, {reuseaddr, true}]),
  DevicesInfo = json_interpreter:parse_file(?DevicesFileName),  % carrega os dados dos dispositivos para memória
  spawn(fun() -> acceptor(LSock, DevicesInfo) end),
  started.
  %timer:send_after(?CollectTime, aggregator),  % começa um timer para dps enviar o q tem para o agregador
  


acceptor(LSock, DevicesInfo) ->
  {ok, Sock} = gen_tcp:accept(LSock),
  io:fwrite("\nNew device connected to socket: ~p.\n", [Sock]),
  % fica à espera de uma nova conexão
  Pid = spawn(fun() -> handle_device(Sock, #{eventsList=>[], online=>true}, null, DevicesInfo) end),
  gen_tcp:controlling_process(Sock, Pid),
  acceptor(LSock, DevicesInfo).
  


% State = #{eventsList=>List, online=>Bool}, TRef é a referencia para o timer de inatividade
handle_device(Sock, State, TRef, DevicesInfo) ->
  receive
    {tcp, _, Data} ->
      inet:setopts(Sock, [{active, once}]),
      Msg = binary_to_term(Data),
      case maps:get(mode, Msg) of
        auth -> 
          io:fwrite("\nColetor recebeu auth info ~p .\n", [Msg]),
          login(maps:get(id, Msg), maps:get(password, Msg), DevicesInfo),
          timer:send_after(?CollectTime, aggregator), % começa aqui o timer para depois enviar info ao agregador
          handle_device(Sock, State, TRef, DevicesInfo);

        event -> 
          Event = maps:get(event_type, Msg),
          DeviceId = maps:get(id, Msg),
          io:fwrite("\nColetor recebeu evento: ~p do device ~p .\n", [Event, DeviceId]),

          timer:cancel(TRef),  % vai criar um novo timer para a questao da atividade
          {ok, NewTRef} = timer:send_after(?AliveTime, alive_timeout),   

          maps:update(online, true, State),
          maps:update(eventsList, maps:get(eventsList, State) ++ Event, State),
          handle_device(Sock, State, NewTRef, DevicesInfo);

        _ -> io:fwrite("\nMensagem inválida!\n")
      end;

    {tcp_closed, _} ->
      io:fwrite("\nConnection closed.\n");

    {tcp_error, _, _} ->
      io:fwrite("\nConnection error.\n");

    auth_ok ->
      io:fwrite("\nAuthentication was successful.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_ok)),  %enviar para o device a confirmaçao de auth
      {ok, NewTRef} = timer:send_after(?AliveTime, alive_timeout),  % começar o timer para ver se está vivo
      handle_device(Sock, State, NewTRef, DevicesInfo);

    auth_error ->
      io:fwrite("\nFailed to authenticate.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_error)),
      gen_tcp:close(Sock);  % acaba a conexão com o dispositivo

    alive_timeout ->
      io:fwrite("\nDevice has not sent anything in a while. Turning it to offline.\n"),
      maps:update(online, false, State),
      handle_device(Sock, State, TRef, DevicesInfo);

    aggregator ->
      io:fwrite("\nSending info to aggregator.\n"),
      % TODO ENVIAR PARA O AGREGADOR A INFO QUE TEM NO EVENTSLIST - CHUMAK
      timer:send_after(?CollectTime, aggregator),
      maps:update(eventsList, [], State),
      handle_device(Sock, State, TRef, DevicesInfo)
  end.






login(DeviceId, DevicePw, DevicesInfo) ->
  DeviceDictionary = lists:nth(DeviceId, DevicesInfo),
  case maps:find(id, DeviceDictionary) of

    {ok, DeviceId} ->
      case maps:find(password, DeviceDictionary) of
        {ok, DevicePw} ->
          io:fwrite("\nAuth successful.\n"),
          self() ! auth_ok;
        {ok, _} ->
          io:fwrite("\nAuth failed.\n"),
          self() ! auth_error      
      end;
      
    {ok, _} ->
      io:fwrite("\nAuth failed.\n"),
      self() ! auth_error  
  end.