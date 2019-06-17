-module(minidote_system_robustness_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_suite/1, packet_loss_test/1, end_per_suite/1]).

all() -> [
  % packet_loss_test
].

init_per_suite(Config) ->
  NodeConfig = [{a, 10017}, {b, 10018}, {c, 10019}],
  test_setup:start_slaves(Config, NodeConfig).

end_per_suite(Config) ->
  ok = test_setup:stop_slaves(Config),
  application:stop(minidote),
  Config.




packet_loss_test(Config) ->
  Counter = test_setup:counter(),
  [NodeA, NodeB, NodeC] = Nodes = proplists:get_value(nodes, Config),

  % drop some packages:
  DelayFun = fun(_From, _To) ->
    N = test_setup:counter_get(Counter),
    test_setup:counter_inc(Counter, 1),
    case N > 20 andalso N < 100 of
      true -> 10000000;
      false -> 0
    end
  end,
  % Add a delay to each node:
  test_setup:mock_link_layer(Nodes, #{delay => DelayFun, debug => true}),

  Key = {<<"key">>, antidote_crdt_set_go, <<"packet_loss_test">>},

  % add 1000 elements to the set
  NumUpdates = 1000,
  spawn_link(fun() ->
    lists:foldl(fun(I, {V1, V2, V3}) ->

      {ok, Vc1} = call(NodeA, minidote, update_objects, [[{Key, add, {a, I}}], V1]),
      % assign value b on node b
      {ok, Vc2} = call(NodeB, minidote, update_objects, [[{Key, add, {b, I}}], V2]),
      % assign value c on node c
      {ok, Vc3} = call(NodeC, minidote, update_objects, [[{Key, add, {c, I}}], V3]),
      {Vc1, Vc2, Vc3}
    end, {ignore, ignore, ignore}, lists:seq(1, NumUpdates)),
    % Stop dropping packets
    test_setup:counter_inc(Counter, NumUpdates)
  end),

  % eventually all values should be at replica A
  test_setup:eventually(60000, fun() ->
    {ok, [{Key, ValA}], VcA} = rpc:call(NodeA, minidote, read_objects, [[Key], ignore]),
    ct:pal("ValueA = ~w~nVC = ~p", [ValA, VcA]),
    Missing = lists:sublist([{X, I} || X <- [a,b,c], I <- lists:seq(1, NumUpdates)] -- ValA, 10),
    ?assertEqual([], Missing)
  end),
  ok.




call(Node, Mod, Func, Args) ->
  call(Node, Mod, Func, Args, 100).
call(Node, Mod, Func, Args, Tries) ->
  case rpc:call(Node, Mod, Func, Args) of
    {badrpc, Reason} when Tries > 1 ->
      ct:pal("Failed Rpc call: ~p ~p:~p(~p)~n~p", [Node, Mod, Func, Args, Reason]),
      call(Node, Mod, Func, Args);
    Other -> Other
  end.
