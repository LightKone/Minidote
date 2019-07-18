%%
%% Copyright (c) 2019 Georges Younes.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(minidote_tcb_tests_SUITE).

-author("Georges Younes <georges.r.younes@gmail.com").

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-include("minidote.hrl").

-define(PORT, 9000).
-define(MINIDOTE_PORT, 10017).

suite() ->
[{timetrap, {minutes, 2}}].

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    _Config.

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    _Config.

all() ->
    [
     test1
     % test2,
     % test3
    ].


%% ===================================================================
%% Tests.
%% ===================================================================

%% Test causal delivery and stability with full membership
test1(_Config) ->
  test(mode3, fullmesh, 3, 0),
  ok.
test2(_Config) ->
  test(mode3, fullmesh, 3, undefined),
  ok.
test3(_Config) ->
  test(mode3, fullmesh, 3, [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
  ok.

%% Test causal delivery and stability with full membership
test(Mode, Overlay, NodesNumber, Latency) ->
  Options = [{node_number, NodesNumber},
             {overlay, Overlay},
             {latency, Latency},
             {camus_mode, Mode}],
  IdToNode = start(Options),
  ct:pal("started"),
  construct_overlay(Options, IdToNode),
  ct:pal("overlay constructed"),
  %% start causal delivery and stability test
  minidote_tests(IdToNode, Options),
  ct:pal("minidote tests done"),
  stop(IdToNode),
  ct:pal("node stopped"),
  ok.

minidote_tests(IdToNode, Options) ->
  Nodes = [Node || {_Id, Node} <- IdToNode],

  fun_setmembership(Nodes),

  ok = counter_test_local(Nodes),
  ok = counter_test_waiting(Nodes),
  ok = counter_test_with_clock(Nodes),
  ok = mv_register_concurrent(Nodes),
  ok = mv_register_concurrent2(Nodes).

%% ===================================================================
%% Minidote tests.
%% ===================================================================

counter_test_local(Nodes) ->
  [NodeA, _NodeB, _NodeC] = Nodes,
  
  % increment counter by 42
  {ok, Vc} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_local">>}, increment, 42}], ignore]),
  ct:pal("VC = ~p", [Vc]),
  % reading on the same replica returns 42
  {ok, [{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_local">>}, 42}], _Vc2} = rpc:call(NodeA, minidote, read_objects, [[{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_local">>}], Vc]),
  ct:pal("Read 1"),
  ok.

counter_test_waiting(Nodes) ->
  [NodeA, NodeB, _NodeC] = Nodes,

  % increment counter by 42
  {ok, Vc} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_waiting">>}, increment, 42}], ignore]),
  ct:pal("VC = ~p", [Vc]),
  % reading on different replica, should eventually return 42
  test_setup:eventually(fun() ->
    {ok, [{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_waiting">>}, 42}], _Vc3} = rpc:call(NodeB, minidote, read_objects, [[{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_waiting">>}], Vc])
  end),
  ok.

counter_test_with_clock(Nodes) ->
  [NodeA, NodeB, _NodeC] = Nodes,

  % increment counter by 42
  {ok, Vc} = rpc:call(NodeA, minidote, update_objects, [[{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_with_clock">>}, increment, 42}], ignore]),
  ct:pal("VC = ~p", [Vc]),
  % reading on different replica, should also return 42, since we provide the vectorclock:
  {ok, [{{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_with_clock">>}, 42}], _Vc3} = rpc:call(NodeB, minidote, read_objects, [[{<<"key">>, antidote_crdt_counter_pn, <<"counter_test_with_clock">>}], Vc]),
  ct:pal("Read 2"),
  ok.

mv_register_concurrent(Nodes) ->
  [NodeA, NodeB, NodeC] = Nodes,

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

mv_register_concurrent2(Nodes) ->
  [NodeA, NodeB, NodeC] = Nodes,

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

%% ===================================================================
%% Internal functions.
%% ===================================================================

%% @private
start(Options) ->
  ct:pal("Start begin"),

  ok = start_erlang_distribution(),
  NodeNumber = proplists:get_value(node_number, Options),
  Latency = proplists:get_value(latency, Options),
  Mode = proplists:get_value(camus_mode, Options),

  InitializerFun = fun(I, Acc) ->
    
    Name = get_node_name(I),
    MinidotePort = get_minidote_port(I),
    ErlFlags =
      "-pa " ++ string:join(code:get_path(), " ") ++ " ",
    PrivDir = code:priv_dir(?APP),
    NodeDir = filename:join([PrivDir, Name]),

    %% Start node
    Config = [{kill_if_fail, true},
              {monitor_master, true},
              {init_timeout, 10000},
              {startup_timeout, 10000},
              {startup_functions, [{code, set_path, [codepath()]}]},
              {env, [
                {"MINIDOTE_PORT", integer_to_list(MinidotePort)},
                {"LOG_DIR", NodeDir}
                ]},
              {erl_flags, ErlFlags}],

    case ct_slave:start(Name, Config) of
      {ok, Node} ->
        orddict:store(I, Node, Acc);
      Error ->
        ct:fail(Error)
    end
  end,

  IdToNode = lists:foldl(InitializerFun,
                         orddict:new(),
                         lists:seq(0, NodeNumber - 1)),

  LoaderFun = fun({Id, Node}) ->

    %% Load camus
    ok = rpc:call(Node, application, load, [camus]),

    %% Load camus_exp
    ok = rpc:call(Node, application, load, [?APP])

  end,
  lists:foreach(LoaderFun, IdToNode),

  ConfigureFun = fun({Id, Node}) ->
    %% Configure camus
    lists:foreach(
      fun({Property, Value}) ->
        ok = rpc:call(Node,
                      camus_config,
                      set,
                      [Property, Value])
      end,
      [{node_number, NodeNumber},
       {camus_port, get_port(Id)},
       {camus_latency, Latency},
       {camus_mode, Mode}]
    )
    end,
    lists:foreach(ConfigureFun, IdToNode),

  StartFun = fun({_Id, Node}) ->
    {ok, _} = rpc:call(Node,
                       application,
                       ensure_all_started,
                       [?APP])
  end,
  lists:foreach(StartFun, IdToNode),

  {ok, Pid} = antidotec_pb_socket:start("localhost", ?MINIDOTE_PORT),
  Disconnected = antidotec_pb_socket:stop(Pid),
  ?assertMatch(ok, Disconnected),

  ct:pal("Start end"),

  IdToNode.

%% @private
construct_overlay(Options, IdToNode) ->
  IdToNodeSpec = lists:map(
    fun({Id, Node}) ->
      Spec = rpc:call(Node, camus_ps, myself, []),
      {Id, Spec}
    end,
    IdToNode
  ),

  NodeNumber = orddict:size(IdToNode),
  Overlay = proplists:get_value(overlay, Options),
  Graph = get_overlay(Overlay, NodeNumber),

  lists:foreach(
    fun({I, Peers}) ->
      Node = orddict:fetch(I, IdToNode),

      lists:foreach(
        fun(Peer) ->
          PeerSpec = orddict:fetch(Peer, IdToNodeSpec),

          ok = rpc:call(Node,
                        camus_ps,
                        join,
                        [PeerSpec])
        end,
        Peers
      )
    end,
    Graph
  ).

%% @private Stop nodes.
stop(IdToNode) ->
  StopFun = fun({I, _Node}) ->
    Name = get_node_name(I),
    case ct_slave:stop(Name) of
      {ok, _} ->
        ok;
      Error ->
        ct:fail(Error)
    end
  end,
  lists:foreach(StopFun, IdToNode).

%% @private Start erlang distribution.
start_erlang_distribution() ->
  os:cmd(os:find_executable("epmd") ++ " -daemon"),
  {ok, Hostname} = inet:gethostname(),
  case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
    {ok, _} ->
      ok;
    {error, {already_started, _}} ->
      ok
  end.

%% @private
codepath() ->
  lists:filter(fun filelib:is_dir/1, code:get_path()).

%% @private
get_node_name(I) ->
  list_to_atom("n" ++ integer_to_list(I)).

%% @private
get_port(Id) ->
  5000 + Id.

%% @private
get_minidote_port(I) ->
  ?MINIDOTE_PORT + I.

%% @private
get_overlay(_, 1) ->
  [];
get_overlay(fullmesh, N) ->
  All = lists:seq(0, N - 1),
  lists:foldl(
    fun(I, Acc) ->
      orddict:store(I, All -- [I], Acc)
    end,
    orddict:new(),
    All
  ).

%% private   
fun_setmembership(Nodes) ->   
  lists:foreach(fun(Node) ->    
      ok = rpc:call(Node, camus, setmembership, [Nodes])    
  end, Nodes).
