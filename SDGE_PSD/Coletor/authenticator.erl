-module(authenticator).
-export([login/3]).
-define(DevicesInfo, load_devices()).

% Devices = Map{ key: device_id, value: {device_type, device_pw}}

login(DeviceId, DevicePw, From) ->
  case maps:find(DeviceId, ?DevicesInfo) of
    {ok, {_, DevicePw}} ->
      From ! auth_ok;

    {ok,_} ->
      From ! auth_error     %TODO pensar o que enviar de volta com o erro para identificar o device
  end.



load_devices() ->
  {ok, File} = file:read_file("Ficheiros/dispositivos.json"),
  MyJSON = unicode:characters_to_list(File),
  jsx:decode(MyJSON, [return_maps]).  % transforma o json num mapa e retorna isso, que fica guardado em DevicesInfo


%rpc -> Remote Procedure Call
rpc(Req) ->
  ?MODULE ! {Req, self()}, 
  receive {Res, ?MODULE} -> Res end.