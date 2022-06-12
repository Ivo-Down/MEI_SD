%%%-------------------------------------------------------------------
%% @doc starter public API
%% @end
%%%-------------------------------------------------------------------

-module(starter_app).
-behaviour(application).
-export([start/2, stop/1]).

-define(NumberOfRegions, 5).
-define(DevicesFileName, "dispositivos.json").
-define(BaseCollectorPort, 1233).
-define(BaseAggregatorPort, 8300).

start(_StartType, _StartArgs) ->
    {CollectorList, AggList} = portList(?NumberOfRegions, [], []),
    io:fwrite("Ports ~p ~p \n", [CollectorList, AggList]),
    login_manager:start(?DevicesFileName),
    start_collectors(CollectorList, AggList),
    devices:start(CollectorList, ?DevicesFileName),
    starter_sup:start_link().

start_collectors([],[]) ->
    done;
start_collectors([CollPort|CT], [AggPort|AT]) ->
    io:fwrite("SC ~p ~p \n",[CollPort,AggPort]),
    coletor:start(CollPort, AggPort, ?DevicesFileName),
    start_collectors(CT, AT).

portList(0, CollList, AggList) -> {CollList, AggList};
portList(NumberOfRegions, CollList, AggList) ->
    NewCollPort = ?BaseCollectorPort + NumberOfRegions,
    NewAggPort = ?BaseAggregatorPort + NumberOfRegions,
    portList(NumberOfRegions-1, [NewCollPort] ++ CollList,[NewAggPort] ++ AggList).
        

stop(_State) ->
    ok.

%% internal functions
