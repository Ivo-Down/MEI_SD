-module(coletor).
-export([start/1]).
-define(CollectTime, 10000).


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
  handle_device(Sock, []).


handle_device(Sock, EventsList) ->
  receive
    {tcp, _, Data} ->
      inet:setopts(Sock, [{active, once}]),
      Msg = binary_to_term(Data),
      case maps:get(mode, Msg) of
        auth -> 
          io:fwrite("\nColetor recebeu auth info ~p .\n", [Msg]),
          %authenticator:login(maps:get(dev_id, Msg), maps:get(dev_password, Msg), ?MODULE), TODO ainda ta por testar
          handle_device(Sock, EventsList);

        event -> 
          Event = maps:get(event_type, Msg),
          DeviceId = maps:get(dev_id, Msg),
          io:fwrite("\nColetor recebeu evento: ~p do device ~p .\n", [Event, DeviceId]),
          EventsList = EventsList ++ Event,
          handle_device(Sock, EventsList);

        _ -> io:fwrite("\nMensagem inválida!\n")
      end;

    {tcp_closed, _} ->
      io:fwrite("\nConnection closed.\n");

    {tcp_error, _, _} ->
      io:fwrite("\nConnection error.\n");

    {auth_ok} ->
      io:fwrite("\nAuthentication was successful.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_ok));  %enviar para o device a confirmaçao de auth

    {auth_error} ->
      io:fwrite("\nFailed to authenticate.\n"),
      ok = gen_tcp:send(Sock, atom_to_binary(auth_error)),
      gen_tcp:close(Sock);  % acaba a conexão com o dispositivo

    {aggregator} ->
      io:fwrite("\nSending info to aggregator.\n"),
      % TODO ENVIAR PARA O AGREGADOR A INFO QUE TEM NO EVENTSLIST
      timer:send_after(?CollectTime, aggregator),
      handle_device(Sock, [])
  end.
