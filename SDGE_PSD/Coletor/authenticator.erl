-module(authenticator).
-export([login/4]).

% Devices = Map{ key: device_id, value: {device_type, device_pw}}

login(DeviceId, DevicePw, From, Devices) ->
  case maps:find(DeviceId, Devices) of
    {ok, {_, DevicePw}} ->
      From ! ok;

    {ok,_} ->
      From ! error
  end.