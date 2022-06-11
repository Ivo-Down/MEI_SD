-module(login_manager).
-export([start/1, login/4]).
%maps

%registar pid do login manager

%interface functions
start(DevicesFile) ->
  % Carregar os dados dos dispositivos para memória
    DevicesInfo = json_interpreter:parse_file(DevicesFile),
    register(?MODULE, spawn(fun() -> loop(DevicesInfo) end)).


login(Username, Password, DeviceType, From) ->
    io:fwrite("Login\n"),
    ?MODULE ! {login, Username, Password, DeviceType, From}.

%logout(Username, From) -> 
%    ?MODULE ! {logout, Username, self()},
%    ok.

%server process
loop(DevicesInfo) ->
    receive
    {login, DeviceId, DevicePw, DeviceType, From} ->
        io:fwrite("Login in loop\n"),
        DeviceDictionary = lists:nth(DeviceId, DevicesInfo),
            case maps:find(id, DeviceDictionary) of
                {ok, DeviceId} ->
                    case maps:find(password, DeviceDictionary) of
                        {ok, DevicePw} ->
                            io:fwrite("\nAuth success.\n"),
                            From ! {auth_ok, DeviceId, DeviceType},
                            loop(DevicesInfo);
                        {ok, _} ->
                            io:fwrite("\nAuth failed.\n"),
                            From ! auth_error,
                            loop(DevicesInfo)
                    end;
                
                {ok, _} ->
                    io:fwrite("\nAuth failed.\n"),
                    From ! auth_error,
                    loop(DevicesInfo)

            end;
        %{logout, Username, From} ->
        %    case maps:is_key(Username, Map) of
        %        true -> From ! {ok, ?MODULE},loop(maps:update_with(Username, fun({P, _}) -> {P, false} end, Map));
        %        false -> From ! {ok, ?MODULE},loop(DevicesInfo)
        %    end;
        M ->
            error_logger:error_report(M)
    end.