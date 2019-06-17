-module(minidote_op_log_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-export([all/0, log_test/1, log_recovery_test/1, init_per_suite/1, end_per_suite/1, parallel_log_test/1, parallel_log_test2/1]).

all() -> [log_test, log_recovery_test, parallel_log_test, parallel_log_test2].

init_per_suite(Config) ->
  Config.

end_per_suite(Config) ->
  Config.

log_test(_Config) ->
  {ok, LogServer} = minidote_op_log:start_link(log_test, self()),
  Proc = fun(Dc) ->
    lists:foreach(fun(J) ->
      ok = minidote_op_log:add_log_entry(LogServer, Dc, {J, {data, Dc, J}})
    end, lists:seq(0, 99))
  end,
  list_utils:pmap(Proc, [dcA, dcB, dcC]),
  {ok, LogEntriesA} = minidote_op_log:read_log_entries(LogServer, dcA, 0, 10, fun(D, Acc) -> Acc ++ [D] end, []),
  ?assertEqual([{I, {data, dcA, I}} || I <- lists:seq(0, 10)], LogEntriesA),
  {ok, LogEntriesB} = minidote_op_log:read_log_entries(LogServer, dcB, 0, 10, fun(D, Acc) -> Acc ++ [D] end, []),
  ?assertEqual([{I, {data, dcB, I}} || I <- lists:seq(0, 10)], LogEntriesB),
  {ok, LogEntriesC} = minidote_op_log:read_log_entries(LogServer, dcC, 0, 10, fun(D, Acc) -> Acc ++ [D] end, []),
  ?assertEqual([{I, {data, dcC, I}} || I <- lists:seq(0, 10)], LogEntriesC),
  ok.


parallel_log_test(_Config) ->
  {ok, LogServer} = minidote_op_log:start_link(parallel_log_test, self()),
  Dc = dcA,
  Proc = fun(I) ->
    ok = minidote_op_log:add_log_entry(LogServer, Dc, {I, {data, I}})
  end,
  list_utils:pmap(Proc, lists:seq(0, 100)),
  {ok, LogEntriesA} = minidote_op_log:read_log_entries(LogServer, dcA, 0, 100, fun(D, Acc) -> Acc ++ [D] end, []),
  ?assertEqual([{I, {data, I}} || I <- lists:seq(0, 100)], LogEntriesA),
  ok.

parallel_log_test2(_Config) ->
  % parallel read
  {ok, LogServer} = minidote_op_log:start_link(parallel_log_test2, self()),
  Dc = dcA,
  Proc = fun(I) ->
    ok = minidote_op_log:add_log_entry(LogServer, Dc, {I, {data, I}})
  end,
  spawn_link(fun() ->
    {ok, LogEntries1} = minidote_op_log:read_log_entries(LogServer, dcA, 0, 100, fun(D, Acc) -> Acc ++ [D] end, []),
    ct:pal("Log entries 1 = ~p", [LogEntries1]),
    {ok, LogEntries2} = minidote_op_log:read_log_entries(LogServer, dcA, 0, 100, fun(D, Acc) -> Acc ++ [D] end, []),
    ct:pal("Log entries 2 = ~p", [LogEntries2])
  end),
  list_utils:pmap(Proc, lists:seq(0, 100)),
  {ok, LogEntriesA} = minidote_op_log:read_log_entries(LogServer, dcA, 0, 100, fun(D, Acc) -> Acc ++ [D] end, []),
  ?assertEqual([{I, {data, I}} || I <- lists:seq(0, 100)], LogEntriesA),
  ok.



log_recovery_test(_Config) ->
  {ok, LogServer} = minidote_op_log:start_link(log_recovery_test, self()),
  ?assertEqual([], receive_recovery()),
  Proc = fun(Server, Start, End) ->
    fun(Dc) ->
      {Dc, lists:map(fun(J) ->
        Entry = {J, {data, Dc, J}},
        ok = minidote_op_log:add_log_entry(Server, Dc, Entry),
        Entry
      end, lists:seq(Start, End))}
    end
  end,
  Entries = list_utils:pmap(Proc(LogServer, 0, 10), [dcA, dcB, dcC]),
  unlink(LogServer),
  exit(LogServer, kill),
  {ok, LogServer2} = minidote_op_log:start_link(log_recovery_test, self()),
  Recovered = receive_recovery(),
  ?assertEqual(lists:sort(lists:flatmap(fun({Dc, Es}) -> [{Dc, E} || E <- Es] end, Entries)), lists:sort(Recovered)),

  % add some more entries
  list_utils:pmap(Proc(LogServer2, 11, 20), [dcA, dcB, dcC]),
  % read recovered entries
  {ok, LogEntries} = minidote_op_log:read_log_entries(LogServer2, dcA, 0, 20, fun(D, Acc) -> Acc ++ [D] end, []),
  ?assertEqual([{I, {data, dcA, I}} || I <- lists:seq(0, 20)], LogEntries),
  ok.

receive_recovery() ->
  receive
    {log_recovery, Dc, Message} ->
      [{Dc, Message} | receive_recovery()];
    log_recovery_done ->
      [];
    Other ->
      [{unexpected_message, Other} | receive_recovery()]
  after 10000 ->
    [timeout]
  end.