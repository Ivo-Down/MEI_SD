-module(login_manager).
-export([start/1, login/4]).

%interface functions
start(DevicesFile) ->
  % Carregar os dados dos dispositivos para memória
    DevicesInfo = json_interpreter:parse_file(DevicesFile),
    DevicesMap = loadFileToMap(DevicesInfo, #{}),
    io:fwrite("~p \n", [DevicesMap]),
    register(?MODULE, spawn(fun() -> loop(DevicesMap) end)).


loadFileToMap([], Map) ->
    Map;

loadFileToMap([H|T], Map) ->
    {ok, DeviceID} = maps:find(id, H),
    {ok, DevicePassword} = maps:find(password, H),
    {ok, DeviceType} = maps:find(type, H),
    loadFileToMap(T, maps:put(DeviceID, {DevicePassword, erlang:binary_to_atom(DeviceType)}, Map)).

login(Username, Password, DeviceType, From) ->
    ?MODULE ! {login, Username, Password, DeviceType, From}.

%server process
loop(DevicesMap) ->
    receive
    {login, DeviceId, DevicePw, DeviceType, From} ->
            case maps:find(DeviceId, DevicesMap) of
                {ok, {DevicePw, DeviceType}} ->
                    io:fwrite("\nAuth success.\n"),
                    From ! {auth_ok, DeviceId, DeviceType},
                    loop(DevicesMap);
                Other ->
                    io:fwrite("\nAuth failed. ~p\n", [Other]),
                    From ! auth_error,
                    loop(DevicesMap)

            end;
        M ->
            error_logger:error_report(M)
    end.