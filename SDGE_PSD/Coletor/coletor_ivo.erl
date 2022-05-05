-module(coletor_ivo).
-export([start/1]).


start(Port) ->
  {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, 4}, {reuseaddr, true}]),
  spawn(fun() -> acceptor(LSock) end),
  ok.


acceptor(LSock) ->
  {ok, Sock} = gen_tcp:accept(LSock),
  io:fwrite("\nNew device connected to socket: ~p.\n", [Sock]),
  spawn(fun() -> acceptor(LSock) end),
  gen_tcp:controlling_process(Sock, self()),
  handle_device(Sock).


handle_device(Sock) ->
  receive
    {tcp, _, Data} ->
      inet:setopts(Sock, [{active, once}]),
      Msg = binary_to_term(Data),
      case maps:get(mode, Msg) of
        auth -> 
          io:fwrite("\nColetor recebeu auth info ~p .\n", [Msg]),
          handle_device(Sock);

        event -> 
          Event = maps:get(event_type, Msg),
          DeviceId = maps:get(dev_id, Msg),
          io:fwrite("\nColetor recebeu evento: ~p do device ~p .\n", [Event, DeviceId]),
          handle_device(Sock);

        _ -> io:fwrite("\nMensagem inválida!\n")
      end;

    {tcp_closed, _} ->
      io:fwrite("\nConnection closed.\n");

    {tcp_error, _, _} ->
      io:fwrite("\nConnection error.\n")
  end.
