-module(minidote_system_tests_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_suite/1, counter_test_local/1, counter_test_waiting/1, counter_test_with_clock/1, mv_register_concurrent/1, end_per_suite/1, mv_register_concurrent2/1]).

all() -> [
  counter_test_local,
  counter_test_waiting,
  counter_test_with_clock,
  mv_register_concurrent,
  mv_register_concurrent2
].

init_per_suite(Config) ->
  NodeConfig = [{a, 10017}, {b, 10018}, {c, 10019}],
  test_setup:start_slaves(Config, NodeConfig).

end_per_suite(Config) ->
  ok = test_setup:stop_slaves(Config),
  application:stop(minidote),
  Config.


counter_test_local(Config) ->
  [NodeA, _NodeB, _NodeC] = Nodes = proplists:get_value(nodes, Config),
  % debug messages:
  test_setup:mock_link_layer(Nodes, #{debug => true}),

  % increment counter by 42
  {ok, Vc} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_local">>}, increment, 42}], ignore]),
  ct:pal("VC = ~p", [Vc]),
  % reading on the same replica returns 42
  {ok, [{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_local">>}, 42}], _Vc2} = rpc:call(NodeA, minidote, read_objects, [[{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_local">>}], Vc]),
  ct:pal("Read 1"),
  ok.


counter_test_waiting(Config) ->
  [NodeA, NodeB, _NodeC] = Nodes = proplists:get_value(nodes, Config),
  % debug messages:
  test_setup:mock_link_layer(Nodes, #{debug => true}),

  % increment counter by 42
  {ok, Vc} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_waiting">>}, increment, 42}], ignore]),
  ct:pal("VC = ~p", [Vc]),
  % reading on different replica, should eventually return 42
  test_setup:eventually(fun() ->
    {ok, [{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_waiting">>}, 42}], _Vc3} = rpc:call(NodeB, minidote, read_objects, [[{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_waiting">>}], Vc])
  end),
  ok.


counter_test_with_clock(Config) ->
  [NodeA, NodeB, _NodeC] = Nodes = proplists:get_value(nodes, Config),
  % debug messages:
  test_setup:mock_link_layer(Nodes, #{debug => true}),

  % increment counter by 42
  {ok, Vc} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_with_clock">>}, increment, 42}], ignore]),
  ct:pal("VC = ~p", [Vc]),
  % reading on different replica, should also return 42, since we provide the vectorclock:
  {ok, [{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_with_clock">>}, 42}], _Vc3} = rpc:call(NodeB, minidote, read_objects, [[{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_with_clock">>}], Vc]),
  ct:pal("Read 2"),
  ok.

mv_register_concurrent(Config) ->
  [NodeA, NodeB, NodeC] = Nodes = proplists:get_value(nodes, Config),
  % debug messages:
  test_setup:mock_link_layer(Nodes, #{debug => true}),

  % assign value a on node a
  {ok, Vc1} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}, assign, <<"a">>}], ignore]),
  % assign value b on node b
  {ok, Vc2} = rpc:call(NodeB, minidote, update_objects, [[{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}, assign, <<"b">>}], ignore]),
  % assign value c on node c
  {ok, Vc3} = rpc:call(NodeC, minidote, update_objects, [[{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}, assign, <<"c">>}], ignore]),

  % eventually all replicas should have the same value:
  test_setup:eventually(fun() ->
    {ok, [{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}, ValA}], _Vc1} = rpc:call(NodeA, minidote, read_objects, [[{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}], ignore]),
    {ok, [{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}, ValB}], _Vc2} = rpc:call(NodeB, minidote, read_objects, [[{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}], ignore]),
    {ok, [{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}, ValC}], _Vc3} = rpc:call(NodeC, minidote, read_objects, [[{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent">>}], ignore]),
    case lists:usort([ValA, ValB, ValC]) of
      [Val] ->
        ct:pal("Value = ~p", [Val]),
        % one of the three values must be contained in the result
        ?assert(lists:member(<<"a">>, Val) orelse lists:member(<<"b">>, Val) orelse lists:member(<<"c">>, Val));
      _ ->
        throw({'values have not converged', [ValA, ValB, ValC]})
    end
  end),
  ok.


mv_register_concurrent2(Config) ->
  [NodeA, NodeB, NodeC] = Nodes = proplists:get_value(nodes, Config),
  % Add a delay to each node:
  test_setup:mock_link_layer(Nodes, #{delay => 50, debug => true}),


  % assign value a on node a
  {ok, Vc1} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}, assign, <<"A">>}], ignore]),
  % assign value b on node b
  {ok, Vc2} = rpc:call(NodeB, minidote, update_objects, [[{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}, assign, <<"B">>}], ignore]),
  % assign value c on node c
  {ok, Vc3} = rpc:call(NodeC, minidote, update_objects, [[{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}, assign, <<"C">>}], ignore]),

  % eventually all replicas should have the same value:
  test_setup:eventually(fun() ->
    {ok, [{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}, ValA}], VcA} = rpc:call(NodeA, minidote, read_objects, [[{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}], ignore]),
    {ok, [{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}, ValB}], VcB} = rpc:call(NodeB, minidote, read_objects, [[{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}], ignore]),
    {ok, [{{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}, ValC}], VcC} = rpc:call(NodeC, minidote, read_objects, [[{<<"key">>, antidote_crdt_register_mv, <<"mv_register_concurrent2">>}], ignore]),
    ct:pal("ValueA = ~p // ~p", [ValA, VcA]),
    ct:pal("ValueB = ~p // ~p", [ValB, VcB]),
    ct:pal("ValueC = ~p // ~p", [ValC, VcC]),
    case lists:usort([ValA, ValB, ValC]) of
      [Val] ->
        % Since we added a delay of 50ms, the 3 assignments
        % should have happened concurrently, so all values should be present:
        ?assertEqual([<<"A">>, <<"B">>, <<"C">>], Val);
      _ ->
        throw({'values have not converged', [ValA, ValB, ValC]})
    end
  end),
  ok.


