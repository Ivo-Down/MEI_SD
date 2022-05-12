-module(coletor).
-export([start/1]).
-define(CollectTime, 10000).
-define(AliveTime, 60000).
-define(DevicesFileName, "dispositivos.json").


start(Port) ->
  {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, 4}, {reuseaddr, true}]),
  {ok, DevicesInfo} = json_interpreter:parse_file(?DevicesFileName),  % carrega os dados dos dispositivos para memória
  spawn(fun() -> acceptor(LSock, DevicesInfo) end),
  ok.


acceptor(LSock, DevicesInfo) ->
  {ok, Sock} = gen_tcp:accept(LSock),
  io:fwrite("\nNew device connected to socket: ~p.\n", [Sock]),
  % fica à espera de uma nova conexão
  spawn(fun() -> acceptor(LSock, DevicesInfo) end),
  gen_tcp:controlling_process(Sock, self()),
  timer:send_after(?CollectTime, aggregator),  % começa um timer para dps enviar o q tem para o agregador
  handle_device(Sock, #{eventsList=>[], online=>true}, null, DevicesInfo).


% State = #{eventsList=>List, online=>Bool}, TRef é a referencia para o timer de inatividade
handle_device(Sock, State, TRef, DevicesInfo) ->
  receive
    {tcp, _, Data} ->
      inet:setopts(Sock, [{active, once}]),
      Msg = binary_to_term(Data),
      case maps:get(mode, Msg) of
        auth -> 
          io:fwrite("\nColetor recebeu auth info ~p .\n", [Msg]),
          login(maps:get(dev_id, Msg), maps:get(dev_password, Msg), DevicesInfo),
          handle_device(Sock, State, TRef, DevicesInfo);

        event -> 
          Event = maps:get(event_type, Msg),
          DeviceId = maps:get(dev_id, Msg),
          io:fwrite("\nColetor recebeu evento: ~p do device ~p .\n", [Event, DeviceId]),
          time:cancel(TRef),  % vai criar um novo timer para a questao da atividade
          {ok, TRef} = timer:send_after(?AliveTime, alive_timeout),   %TODO confirmar q posso alterar TRef
          maps:update(online, false, State),
          maps:update(eventsList, maps:get(eventsList, State) ++ Event, State),
          handle_device(Sock, State, TRef, DevicesInfo);

        _ -> io:fwrite("\nMensagem inválida!\n")
      end;

    {tcp_closed, _} ->
      io:fwrite("\nConnection closed.\n");

    {tcp_error, _, _} ->
      io:fwrite("\nConnection error.\n");

    {auth_ok} ->
      io:fwrite("\nAuthentication was successful.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_ok)),  %enviar para o device a confirmaçao de auth
      {ok, TRef} = timer:send_after(?AliveTime, alive_timeout),  % começar o timer para ver se está vivo
      handle_device(Sock, State, TRef, DevicesInfo);

    {auth_error} ->
      io:fwrite("\nFailed to authenticate.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_error)),
      gen_tcp:close(Sock);  % acaba a conexão com o dispositivo

    {alive_timeout} ->
      io:fwrite("\nDevice has not sent anything in a while. Turning it to offline\n"),
      maps:update(online, false, State),
      handle_device(Sock, State, TRef, DevicesInfo);

    {aggregator} ->
      io:fwrite("\nSending info to aggregator.\n"),
      % TODO ENVIAR PARA O AGREGADOR A INFO QUE TEM NO EVENTSLIST - CHUMAK
      timer:send_after(?CollectTime, aggregator),
      maps:update(eventsList, [], State),
      handle_device(Sock, State, TRef, DevicesInfo)
  end.






login(DeviceId, DevicePw, DevicesInfo) ->
  case maps:find(DeviceId, DevicesInfo) of
    {ok, {_, DevicePw}} ->
      ?MODULE ! auth_ok;

    {ok,_} ->
      ?MODULE ! auth_error     %TODO pensar o que enviar de volta com o erro para identificar o device
  end.