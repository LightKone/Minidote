-module(list_utils).


-export([pmap/2, group/1, group_by/2, pick_random/1]).

%% like lists:map, but each element is computed in its own process
-spec pmap(fun((A) -> B), [A]) -> [B].
pmap(_, []) ->
  [];
pmap(F, [X]) ->
  [F(X)];
pmap(F, List) ->
  Self = self(),
  Pids = [spawn_link(fun() -> Self!{self(), F(X)} end) || X <- List ],
  [receive {Pid, X} -> X end || Pid <- Pids].

% convert a key-value list to a map and keep duplicates
-spec group([{K,V}]) -> maps:map(K, [V]).
group(L) ->
  group_by(fun({K,_}) -> K end, L).


% group a list by the given function
-spec group_by(fun((V) -> K), [V]) -> maps:map(K, [V]).
group_by(Key, L) ->
  lists:foldr(fun(V, M) ->
    maps:update_with(Key(V), fun(R) -> [V| R] end, [V], M)
  end, maps:new(), L).

% pick a random element from a list
pick_random(List) ->
  Index = rand:uniform(length(List)),
  lists:nth(Index, List).
