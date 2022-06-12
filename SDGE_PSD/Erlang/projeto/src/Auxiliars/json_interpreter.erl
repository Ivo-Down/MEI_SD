-module(json_interpreter).

-export([parse_file/1]).

-export([init/1, handle_event/2]).

parse_file(FN) ->
    {ok, File} = file:read_file(FN),
    Term = jsx:decode(File, [{labels, atom}]),
    Term.

read(File, JSX) ->
    {ok, Data} = file:read(File, 8), %% eof should raise error
    case JSX(Data) of
        {incomplete, F} ->
            read(File, F);
        {with_tail, _, Tail} ->
            Tail =/= <<>> andalso io:format("Surplus content: ~s~n", [Tail])
    end,
    JSX(Data).

init(_) ->
    start.


%% https://learnyousomeerlang.com/event-handlers
handle_event(start_array, start) ->
    [];
handle_event(_, start) ->
    error(expect_array);
handle_event(start_object, L) ->
    [start_object|L];
handle_event(start_array, L) ->
    [start_array|L];
handle_event(end_object, L) ->
    check_out(collect_object(L));
handle_event(end_array, []) ->
    stop;
handle_event(end_array, L) ->
    check_out(collect_array(L));
handle_event(E, L) ->
    check_out([event(E)|L]).

check_out([X]) ->
    io:format("Collected object: ~p~n", [X]),
    [];
check_out(L) -> L.

event({_, X}) -> X;
event(X) -> X.

collect_object(L) ->
    collect_object(L, #{}).

collect_object([start_object|T], M) ->
    [M|T];
collect_object([V, K|T], M) ->
    collect_object(T, M#{K => V}).

collect_array(L) ->
    collect_array(L, []).

collect_array([start_array|T], L) ->
    [L|T];
collect_array([H|T], L) ->
    collect_array(T, [H|L]).