-module(coletor).
-export([start/1]).
-define(CollectTime, 10000).
-define(AliveTime, 60000).


start(Port) ->
  {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, 4}, {reuseaddr, true}]),
  spawn(fun() -> acceptor(LSock) end),
  ok.


acceptor(LSock) ->
  {ok, Sock} = gen_tcp:accept(LSock),
  io:fwrite("\nNew device connected to socket: ~p.\n", [Sock]),
  spawn(fun() -> acceptor(LSock) end),
  gen_tcp:controlling_process(Sock, self()),
  timer:send_after(?CollectTime, aggregator),  % começa um timer para dps enviar o q tem para o agregador
  handle_device(Sock, #{eventsList=>[], online=>true}, null).


% State = #{eventsList=>List, online=>Bool}, TRef é a referencia para o timer de inatividade
handle_device(Sock, State, TRef) ->
  receive
    {tcp, _, Data} ->
      inet:setopts(Sock, [{active, once}]),
      Msg = binary_to_term(Data),
      case maps:get(mode, Msg) of
        auth -> 
          io:fwrite("\nColetor recebeu auth info ~p .\n", [Msg]),
          %authenticator:login(maps:get(dev_id, Msg), maps:get(dev_password, Msg), ?MODULE), TODO ainda ta por testar
          handle_device(Sock, State, TRef);

        event -> 
          Event = maps:get(event_type, Msg),
          DeviceId = maps:get(dev_id, Msg),
          io:fwrite("\nColetor recebeu evento: ~p do device ~p .\n", [Event, DeviceId]),
          time:cancel(TRef),  % vai criar um novo timer para a questao da atividade
          {ok, TRef} = timer:send_after(?AliveTime, alive_timeout),   %TODO confirmar q posso alterar TRef
          maps:update(online, false, State),
          maps:update(eventsList, maps:get(eventsList, State) ++ Event, State),
          handle_device(Sock, State, TRef);

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
      handle_device(Sock, State, TRef);

    {auth_error} ->
      io:fwrite("\nFailed to authenticate.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_error)),
      gen_tcp:close(Sock);  % acaba a conexão com o dispositivo

    {alive_timeout} ->
      io:fwrite("\nDevice has not sent anything in a while. Turning it to offline\n"),
      maps:update(online, false, State),
      handle_device(Sock, State, TRef);

    {aggregator} ->
      io:fwrite("\nSending info to aggregator.\n"),
      % TODO ENVIAR PARA O AGREGADOR A INFO QUE TEM NO EVENTSLIST
      timer:send_after(?CollectTime, aggregator),
      maps:update(eventsList, [], State),
      handle_device(Sock, State, TRef)
  end.
