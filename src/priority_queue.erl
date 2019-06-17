-module(priority_queue).

%% priority queue implemented using a skew heap

%% API
-export([new/0, union/2, in/2, is_empty/1, peek/1, out/1, singleton/1, peek_list/1, drop/1, is_queue/1, from_list/1, len/1, to_list/1]).

-export_type([pq/1]).

-type pq(T) :: empty | {node, T, pq(T), pq(T)}.

-spec new() -> pq(any()).
new() -> empty.

singleton(T) -> {node, T, empty, empty}.

from_list(List) ->
  lists:foldl(fun in/2, new(), List).

to_list(T) ->
  case out(T) of
    {{value,X}, R} ->
      [X|to_list(R)];
    {empty, _} ->
      []
  end.

-spec union(pq(T), pq(T)) -> pq(T).
union(T, empty) -> T;
union(empty, T) -> T;
union(T1={node, X1, L1, R1}, T2={node, X2, L2, R2}) ->
  case X1 =< X2 of
    true ->
      {node, X1, union(T2, R1), L1};
    false ->
      {node, X2, union(T1, R2), L2}
  end.

is_queue(empty) -> true;
is_queue({node, _, L, R}) -> is_queue(L) andalso is_queue(R);
is_queue(_) -> false.

-spec in(T, pq(T)) -> pq(T).
in(X, T) ->
  union(singleton(X), T).

-spec out(pq(T)) -> {{value, T}, pq(T)} | {empty, pq(T)}.
out(empty) ->
  {empty, empty};
out({node, E, L, R}) ->
  {{value, E}, union(L, R)}.

-spec drop(pq(T)) -> pq(T).
drop(empty) ->
  empty;
drop({node, _, L, R}) ->
  union(L, R).


-spec peek(pq(T)) -> {value, T} | empty.
peek(empty) ->
  empty;
peek({node, E, _, _}) ->
  {value, E}.

-spec peek_list(pq(T)) -> [] | [T].
peek_list(empty) ->
  [];
peek_list({node, E, _, _}) ->
  [E].

-spec is_empty(pq(any())) -> boolean().
is_empty(T) ->
  T == empty.

len(empty) ->
  0;
len({node, _, L, R}) ->
  1 + len(L) + len (R).