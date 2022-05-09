%%%-------------------------------------------------------------------
%% @doc leitor public API
%% @end
%%%-------------------------------------------------------------------

-module(leitor_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    leitor_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
