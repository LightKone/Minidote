-module(minidote).

%% API
-export([read_objects/2, update_objects/2]).

-export_type([key/0]).

-type key() :: {Key :: binary(), Type :: antidote_crdt:typ(), Bucket :: binary()}.
-type clock() :: any().

-spec read_objects([key()], clock()) ->
  {ok, [any()], clock()}
  | {error, any()}.
read_objects(Objects, Clock) ->
  lager:info("read_objects(~p, ~p)", [Objects, Clock]),
  minidote_server:read_objects(minidote_server, Objects, Clock).

-spec update_objects([{key(), Op :: atom(), Args :: any()}], clock()) ->
    {ok, clock()}
  | {error, any()}.
update_objects(Updates, Clock) ->
  lager:info("update_objects(~p, ~p)", [Updates, Clock]),
  io:format("io:update_objects(~p, ~p)~n", [Updates, Clock]),
  minidote_server:update_objects(minidote_server, Updates, Clock).
