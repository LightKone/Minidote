-module(minidote_server_SUITE).

% This suite tests the minidote_server module in isolation
% The only dependency of the minidote_server (the minidote_logged_causal_broadcast module) is mocked.


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-export([all/0, read_initial_state/1, init_per_suite/1, end_per_suite/1, update_counter/1, wait_for_clock/1, init_per_testcase/2, end_per_testcase/2, parallel_tests/1, concurrent_updates_test/1]).

all() -> [
  read_initial_state,
  update_counter,
  wait_for_clock,
  concurrent_updates_test,
  parallel_tests
].

init_per_suite(Config) ->
  process_flag(trap_exit, true),
  meck:unload(),
  Config.

init_per_testcase(TestCase, Config) ->
  % setup mocks:
  meck:unload(),
  ok = meck:new(minidote_logged_causal_broadcast, [no_link]),
  ok = meck:expect(minidote_logged_causal_broadcast, this_node, fun(_) -> {ok, test_node} end),
  ok = meck:expect(minidote_logged_causal_broadcast, start_link, fun(RespondTo, _Name) ->
    ct:pal("minidote_logged_causal_broadcast start_link called~n"),
    % no log to recover -> send 'done'
    RespondTo ! log_recovery_done,
    {ok, logged_causal_broadcast_mock}
  end),
  meck:expect(minidote_logged_causal_broadcast, broadcast, fun(B, Message) ->
    ct:pal("Broadcast0 ~p:~n ~p~n", [B, Message]),
    ok
  end),
  meck:expect(minidote_logged_causal_broadcast, stop, fun(_Pid) -> ok end),
  Config.

end_per_testcase(TestCase, Config) ->
  true = meck:validate(minidote_logged_causal_broadcast),
  meck:unload(),
  receive_messages(),
  Config.


end_per_suite(Config) ->
  Config.

% Reading a nonexisting key should return the inital value:
read_initial_state(_Config) ->
  {ok, Server} = minidote_server:start_link(minidote_test_server1),
  Key = {<<"key1">>, antidote_crdt_counter_pn, <<"bucket1">>},
  {ok, [Value], _Clock} = minidote_server:read_objects(Server, [Key], ignore),
  ?assertEqual({Key, 0}, Value),
  minidote_server:stop(Server),
  ok.


% This tests a single update and read.
% It also tests, that the update only returns, after the minidote_logged_causal_broadcast has confirmed that
% it is stored persistently.
update_counter(_Config) ->
  {ok, Server} = minidote_server:start_link(minidote_test_server2),
  Key = {<<"key1">>, antidote_crdt_counter_pn, <<"bucket1">>},
  P = spawn_link(fun() ->
    receive
      {broadcast, S, _Message} ->
        S ! {self(), ack};
      update_objects_responded ->
        throw("minidote_server:update_objects responded before broadcast was complete")
    after 1000 ->
      throw("minidote_server:update_objects took too long")
    end
  end),
  meck:expect(minidote_logged_causal_broadcast, broadcast, fun(B, Message) ->
    ct:pal("Broadcast1 ~p: ~p~n", [B, Message]),
    P ! {broadcast, self(), Message},
    receive {P, ack} -> ok end
  end),
  {ok, Clock} = minidote_server:update_objects(Server, [{Key, increment, 1}], ignore),
  P ! update_objects_responded,
  % read value after update
  {ok, [Value], _Clock} = minidote_server:read_objects(Server, [Key], Clock),
  ?assertEqual({Key, 1}, Value),
  minidote_server:stop(Server).

% Tests that responses can wait for a Clock from a different server
wait_for_clock(_Config) ->
  TestProcess = self(),
  ok = meck:expect(minidote_logged_causal_broadcast, this_node, fun
    (logged_causal_broadcast_mock1) -> {ok, test_node1};
    (logged_causal_broadcast_mock2) -> {ok, test_node2}
  end),
  ok = meck:expect(minidote_logged_causal_broadcast, start_link, fun(RespondTo, _Name) ->
    % no log to recover -> send 'done'
    RespondTo ! log_recovery_done,
    TestProcess ! {respond_to1, RespondTo},
    {ok, logged_causal_broadcast_mock1}
  end),
  {ok, Server1} = minidote_server:start_link(minidote_test_server3),
  Server1RespondTo = receive {respond_to1, R1} -> R1 end,
  ok = meck:expect(minidote_logged_causal_broadcast, start_link, fun(RespondTo, _Name) ->
    % no log to recover -> send 'done'
    RespondTo ! log_recovery_done,
    TestProcess ! {respond_to2, RespondTo},
    {ok, logged_causal_broadcast_mock2}
  end),
  {ok, Server2} = minidote_server:start_link(minidote_test_server4),
  Server2RespondTo = receive {respond_to2, R2} -> R2 end,
  meck:expect(minidote_logged_causal_broadcast, broadcast, fun(B, Message) ->
    ct:pal("Broadcast2 ~p: ~p~n", [B, Message]),
    TestProcess ! {broadcast, B, Message},
    ok
  end),

  % update on server1:
  Key = {<<"key1">>, antidote_crdt_counter_pn, <<"bucket1">>},
  {ok, Clock} = minidote_server:update_objects(Server1, [{Key, increment, 1}], ignore),

  % read on server2 with clock (in new process, because it has to wait):
  spawn_link(fun() ->
    {ok, [Value], _Clock} = minidote_server:read_objects(Server2, [Key], Clock),
    TestProcess ! {read1_result, Value}
  end),

  % read on server2 without clock:
  {ok, [Value], _Clock} = minidote_server:read_objects(Server2, [Key], ignore),
  %should read the old value
  ?assertEqual({Key, 0}, Value),

  % now deliver broadcast messages
  loop(fun() ->
    receive
      {broadcast, logged_causal_broadcast_mock1, M} ->
        ct:pal("Deliver 1 -> 2: ~p", [M]),
        Server2RespondTo ! {deliver, M};
      {broadcast, logged_causal_broadcast_mock2, M} ->
        ct:pal("Deliver 2 -> 1: ~p", [M]),
        Server1RespondTo ! {deliver, M}
    after 500 ->
      ct:pal("Delivered all"),
      return
    end
  end),

  Value2 = receive
    {read1_result, V2} -> V2
  after 500 ->
      timeout
  end,
  ?assertEqual({Key, 1}, Value2).


% Tests that responses can wait for a Clock from a different server
concurrent_updates_test(_Config) ->
  TestProcess = self(),
  ok = meck:expect(minidote_logged_causal_broadcast, this_node, fun
    (logged_causal_broadcast_mock1) -> {ok, test_node1};
    (logged_causal_broadcast_mock2) -> {ok, test_node2}
  end),
  ok = meck:expect(minidote_logged_causal_broadcast, start_link, fun(RespondTo, _Name) ->
    % no log to recover -> send 'done'
    RespondTo ! log_recovery_done,
    TestProcess ! {respond_to1, RespondTo},
    {ok, logged_causal_broadcast_mock1}
  end),
  {ok, Server1} = minidote_server:start_link(minidote_test_server5),
  Server1RespondTo = receive {respond_to1, R1} -> R1 end,
  ok = meck:expect(minidote_logged_causal_broadcast, start_link, fun(RespondTo, _Name) ->
    % no log to recover -> send 'done'
    RespondTo ! log_recovery_done,
    TestProcess ! {respond_to2, RespondTo},
    {ok, logged_causal_broadcast_mock2}
  end),
  {ok, Server2} = minidote_server:start_link(minidote_test_server6),
  Server2RespondTo = receive {respond_to2, R2} -> R2 end,
  meck:expect(minidote_logged_causal_broadcast, broadcast, fun(B, Message) ->
    ct:pal("Broadcast2 ~p: ~p~n", [B, Message]),
    TestProcess ! {broadcast, B, Message},
    ok
  end),

  Key = {<<"key1">>, antidote_crdt_register_mv, <<"bucket1">>},

  % update on server1:
  {ok, Clock1} = minidote_server:update_objects(Server1, [{Key, assign, <<"x">>}], ignore),

  % update on server2:
  {ok, Clock2} = minidote_server:update_objects(Server2, [{Key, assign, <<"y">>}], ignore),

  % read on server1 with clock2 (in new process, because it has to wait):
  spawn_link(fun() ->
    {ok, [Value], _Clock} = minidote_server:read_objects(Server1, [Key], Clock2),
    TestProcess ! {read1_result, Value}
  end),

  % read on server2 with clock1 (in new process, because it has to wait):
  spawn_link(fun() ->
    {ok, [Value], _Clock} = minidote_server:read_objects(Server2, [Key], Clock1),
    TestProcess ! {read2_result, Value}
  end),

  % read on server2 without clock:
  {ok, [Value], _Clock} = minidote_server:read_objects(Server2, [Key], ignore),
  %should read the value "y" that it wrote itself
  ?assertEqual({Key, [<<"y">>]}, Value),

  % now deliver broadcast messages
  loop(fun() ->
    receive
      {broadcast, logged_causal_broadcast_mock1, M} ->
        ct:pal("Deliver 1 -> 2: ~p", [M]),
        Server2RespondTo ! {deliver, M};
      {broadcast, logged_causal_broadcast_mock2, M} ->
        ct:pal("Deliver 2 -> 1: ~p", [M]),
        Server1RespondTo ! {deliver, M}
    after 500 ->
      ct:pal("Delivered all"),
      return
    end
  end),

  Value1 = receive
    {read1_result, V1} -> V1
  after 500 ->
      timeout
  end,
  % since the updates were concurrent, both values should be read:
  ?assertEqual({Key, [<<"x">>, <<"y">>]}, Value1),
  % same result for server 2:
  Value2 = receive
    {read2_result, V2} -> V2
  after 500 ->
      timeout
  end,
  ?assertEqual({Key, [<<"x">>, <<"y">>]}, Value2).


% Tests that the server can handle requests done in parallel
parallel_tests(_Config) ->
  NumKeys = 3,
  NumProcesses = 3,
  IncrementsPerKeyAndProcess = 2,
  {ok, Server} = minidote_server:start_link(minidote_test_server5),
  P = fun(_) ->
    % each process increments key1-key10 10 times
    lists:map(fun(_) ->
      lists:map(fun(I) ->
        KeyI = integer_to_binary(I),
        Key = {<<"key", KeyI/binary>>, antidote_crdt_counter_pn, <<"bucket1">>},
        {ok, _} = minidote_server:update_objects(Server, [{Key, increment, 1}], ignore)
      end, lists:seq(1, NumKeys))
    end, lists:seq(1, IncrementsPerKeyAndProcess))
  end,
  % 10 parallel processes
  list_utils:pmap(P, lists:seq(1, NumProcesses)),
  % every counter should have value zero
  Keys = lists:map(fun(I) -> KeyI = integer_to_binary(I), {<<"key", KeyI/binary>>, antidote_crdt_counter_pn, <<"bucket1">>} end, lists:seq(1, NumKeys)),
  {ok, Values, _Clock} = minidote_server:read_objects(Server, Keys, ignore),
  ?assertEqual([{K, IncrementsPerKeyAndProcess * NumProcesses} || K <- Keys], Values),
  ok.


receive_messages() ->
  receive
    X ->
      ct:pal("Received ~p", [X]),
      receive_messages()
  after 0 ->
    ok
  end.

loop(F) ->
  case F() of
    return -> ok;
    _ -> loop(F)
  end.