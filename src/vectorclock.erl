-module(vectorclock).

-export([new/0, increment/2, get/2, leq/2, merge/2, from_list/1, to_list/1, leq_extra/2]).

-export_type([vectorclock/0]).

-type vectorclock() :: maps:map(any(), pos_integer()).

-spec new() -> vectorclock().
new() ->
  maps:new().

from_list(List) ->
  maps:from_list(List).

to_list(VC) ->
  maps:to_list(VC).

increment(VC, P) ->
  maps:put(P, get(VC, P) + 1, VC).

get(VC, P) ->
  maps:get(P, VC, 0).

leq(VC1, VC2) ->
  maps:fold(fun(P, V, Res) -> Res andalso get(VC2, P) >= V end, true, VC1).

leq_extra(VC1, VC2) ->
  maps:fold(fun(P, V, Res) ->
    case Res of
      true ->
        case get(VC2, P) >= V of
          true -> true;
          false -> {false, P, V}
        end;
      _ -> Res
    end
  end, true, VC1).

merge(VC1, VC2) ->
  M = maps:merge(VC1, VC2),
  maps:fold(fun(P, V, Res) ->
    maps:put(P, max(V, get(Res, P)), Res)
  end, M, VC1).

