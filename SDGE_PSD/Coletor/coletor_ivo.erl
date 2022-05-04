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
    {tcp, _, Data} ->    % reencaminha a msg para ele próprio
      inet:setopts(Sock, [{active, once}]),
      Event = binary_to_atom(Data),
      io:fwrite("\nColetor recebeu ~p .\n", [Event]),
      handle_device(Sock);

    {tcp_closed, _} ->
      io:fwrite("\nConnection closed.\n");

    {tcp_error, _, _} ->
      io:fwrite("\nConnection error.\n")
  end.
