-module(minidote_op_log).
%% The op-log logs operations to disk
%% It keeps one log file per originating data-center
%% in which operations are kept in a total order
%% (consistent with the order in which the operations were
%% issued at the original data-center.
%% The server replies only after operations have been written to disk.

-behaviour(gen_server).

%% API
-export([start_link/2, add_log_entry/3, read_log_entries/6, stop/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-type server() :: pid().
-type log() :: any().
-type log_entry() :: {Number :: pos_integer(), Data :: any()}.

-record(state, {
  log = maps:new() :: maps:map(node(), log()),
  open_requests = [] :: [{node(), pid()}],
  open_request_count = 0 :: integer(),
  server_name :: atom(),
  max_index = maps:new() :: maps:map(node(), pos_integer()),
  recovery_receiver :: pid(),
  waiting = maps:new() :: maps:map(node(), priority_queue:pq({log_entry(), From :: any()}))
}).

% how many requests to buffer, before calling sync
-define(REQUESTS_PER_SYNC, 100).
-define(MEGABYTES, 1024 * 1024).
-define(MAX_BYTES_PER_LOG_FILE, 500 * ?MEGABYTES).
-define(MAX_LOG_FILES, 64999).

% messages
-record(add_log_entry, {
  dc :: node(),
  entry :: log_entry()
}).

-record(read_log_entries_continuation, {
  log :: disk_log:log(),
  first_index :: non_neg_integer(),
  last_index :: non_neg_integer() | all,
  continuation :: start | {[any()], DiskLogContinuation :: any()}
}).

-record(get_log, {
  dc :: node()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% Starts the server
-spec start_link(atom(), pid()) -> {ok, Pid :: pid()}| {error, Reason :: term()}.
start_link(ServerName, RecoveryReceiver) ->
  gen_server:start_link(?MODULE, [ServerName, RecoveryReceiver], []).

stop(Pid) ->
  gen_server:stop(Pid).

%% Add a log entry to the end of the log
-spec add_log_entry(server(), node(), log_entry()) -> ok  | {error, Reason :: term()}.
add_log_entry(Server, Dc, {Index, _Data} = Entry) when is_integer(Index) ->
  gen_server:call(Server, #add_log_entry{dc = Dc, entry = Entry}).

%% Read all log entries in a certain range
-spec read_log_entries(server(), node(), integer(), integer() | all, fun((log_entry(), Acc) -> Acc), Acc) -> {ok, Acc}.
read_log_entries(Server, Dc, FirstIndex, LastIndex, F, Acc) ->
  Continuation = read_log_entries(Server, Dc, FirstIndex, LastIndex),
  read_log_entries_fold(F, Acc, Continuation).

read_log_entries_fold(F, Acc, Continuation) ->
  case read_log_entries_continuation(Continuation) of
    {ok, V, Continuation2} ->
      read_log_entries_fold(F, F(V, Acc), Continuation2);
    eof ->
      {ok, Acc}
  end.

%% Read all log entries in a certain range
%% returns a continuation, which can be used with read_log_entries_continuation
-spec read_log_entries(server(), node(), integer(), integer() | all) -> #read_log_entries_continuation{}.
read_log_entries(Server, Dc, FirstIndex, LastIndex) ->
  {ok, Log} = gen_server:call(Server, #get_log{dc = Dc}),
  #read_log_entries_continuation{log = Log, first_index = FirstIndex, last_index = LastIndex, continuation = start}.


%% returns {ok, Value, Continuation} or eof
-spec read_log_entries_continuation(#read_log_entries_continuation{}) -> {ok, {non_neg_integer(), any()}, #read_log_entries_continuation{}} | eof.
read_log_entries_continuation(#read_log_entries_continuation{first_index = FirstIndex, last_index = LastIndex, continuation = {[{I, V} | R], C}} = Cont) ->
  NewContinuation = Cont#read_log_entries_continuation{continuation = {R, C}},
  case FirstIndex =< I andalso I =< LastIndex of
    true ->
      {ok, {I, V}, NewContinuation};
    false ->
      read_log_entries_continuation(NewContinuation)
  end;
read_log_entries_continuation(#read_log_entries_continuation{log = Log, continuation = Continuation} = Cont) ->
  DiskLogContinuation = case Continuation of
    start -> start;
    {[], C} -> C
  end,
  case chunk(Log, DiskLogContinuation) of
    {DiskLogContinuation2, Terms} ->
      read_log_entries_continuation(Cont#read_log_entries_continuation{continuation = {Terms, DiskLogContinuation2}});
    eof ->
      eof
  end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ServerName, RecoveryReceiver]) ->
  self() ! start_recovery,
  {ok, Dcs} = read_dc_list_from_disk(ServerName),
  Logs = [{Dc, open_log(ServerName, Dc)} || Dc <- Dcs],
  {ok, #state{
    server_name       = ServerName,
    recovery_receiver = RecoveryReceiver,
    log               = maps:from_list(Logs)
  }}.

handle_call(#add_log_entry{dc = Dc, entry = Entry}, From, State) ->
  {Idx, _Data} = Entry,
  MaxIdx = maps:get(Dc, State#state.max_index, -1),
  if
    Idx =< MaxIdx ->
      lager:warning("Index already logged on ~p: ~p/~p: ~n~p", [Dc, Idx, MaxIdx, Entry]),
      {reply, {error, {index_already_logged, Dc, {max, MaxIdx}, {actual, Idx}}}, State, 1};
    true ->
      Queue = maps:get(Dc, State#state.waiting, priority_queue:new()),
      Queue2 = priority_queue:in({Entry, From}, Queue),
      State2 = add_log_entries(State, Dc, Queue2, MaxIdx, []),
      case State2#state.open_request_count > ?REQUESTS_PER_SYNC of
        true ->
          {noreply, do_sync(State2)};
        false ->
          {noreply, State2, 1}
      end
  end;
handle_call(#get_log{dc = Dc}, _From, State) ->
  {Log, State2} = get_log(Dc, State),
  {reply, {ok, Log}, State2, 1}.

handle_cast(Request, State) ->
  lager:warning("Unhandled cast message: ~p~n", [Request]),
  {noreply, State, 1}.

handle_info(timeout, State) ->
  % if there was no request for some time, sync the log and reply to pending
  {noreply, do_sync(State)};
handle_info(start_recovery, State) ->
  % send messages from the log to the broadcast module
  Receiver = State#state.recovery_receiver,
  MaxIndexes = list_utils:pmap(fun({Dc, Log}) ->
    Cont = #read_log_entries_continuation{log = Log, first_index = 0, last_index = all, continuation = start},
    {Dc, recover_log(Dc, Receiver, Cont, 0)}
  end, maps:to_list(State#state.log)),
  Receiver ! log_recovery_done,
  {noreply, State#state{max_index = maps:from_list(MaxIndexes)}}.
% which then will replay them in the right order

terminate(Reason, State) ->
  % close all logs:
  maps:map(fun(_, Log) -> disk_log:close(Log) end, State#state.log).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


get_log(Dc, State) ->
  case maps:find(Dc, State#state.log) of
    {ok, Log} ->
      {Log, State};
    error ->
      Log = open_log(State#state.server_name, Dc),
      KnownDcs = [Dc | maps:keys(State#state.log)],
      ok = write_dc_list_to_disk(State#state.server_name, KnownDcs),
      {Log, State#state{log = maps:put(Dc, Log, State#state.log)}}
  end.

open_log(ServerName, Dc) ->
  Dir = os:getenv("OP_LOG_DIR", "data/op_log/"),
  Filename = filename:join([Dir, "minidote_op_log_" ++ atom_to_list(ServerName), atom_to_list(Dc), "op_log"]),
  ok = filelib:ensure_dir(Filename),
  DiskLogOpts = [
    {name, {minidote_op_log, ServerName, Dc}},
    {file, Filename},
    {type, wrap},
    {size, {?MAX_BYTES_PER_LOG_FILE, ?MAX_LOG_FILES}}],
  case disk_log:open(DiskLogOpts) of
    {ok, Log} -> Log;
    {repaired, Log, {recovered, R}, {badbytes, B}} ->
      lager:warning("Repaired log, recovered ~p with ~p bad bytes~n", [R, B]),
      Log
  end.

chunk(Log, Continuation) ->
  case disk_log:chunk(Log, Continuation) of
    {Cont, Terms, BadBytes} ->
      lager:warning("Read ~p bad bytes from disk log ~p~n", [BadBytes, Log]),
      {Cont, Terms};
    Other ->
      Other
  end.

do_sync(State) ->
  %% sync logs of open requests
  DcsToSync = lists:usort([Dc || {Dc, _} <- State#state.open_requests]),
  list_utils:pmap(fun(Dc) ->
    Log = maps:get(Dc, State#state.log),
    ok = disk_log:sync(Log)
  end, DcsToSync),
  % answer open requests:
  lists:foreach(fun({_, From}) ->
    gen_server:reply(From, ok)
end, State#state.open_requests),
  State#state{
    open_request_count = 0,
    open_requests      = []
  }.


dc_list_file(ServerName) ->
  Dir = os:getenv("OP_LOG_DIR", "data/op_log/"),
  filename:join([Dir, "minidote_op_log_" ++ atom_to_list(ServerName), "nodes.e"]).

-spec read_dc_list_from_disk(atom()) -> {ok, [node()]} | {error, Reason :: any()}.
read_dc_list_from_disk(ServerName) ->
  case file:read_file(dc_list_file(ServerName)) of
    {ok, Bin} ->
      {ok, binary_to_term(Bin)};
    {error, enoent} ->
      % file does not exist, return empty list
      {ok, []};
    Err ->
      Err
  end.

-spec write_dc_list_to_disk(atom(), [node()]) -> ok | {error, Reason :: any()}.
write_dc_list_to_disk(ServerName, Dcs) ->
  ListFile = dc_list_file(ServerName),
  filelib:ensure_dir(ListFile),
  file:write_file(ListFile, term_to_binary(Dcs)).

recover_log(Dc, Receiver, Cont, MaxIndex) ->
  case read_log_entries_continuation(Cont) of
    {ok, Val, NewCont} ->
      {Idx, _} = Val,
      Receiver ! {log_recovery, Dc, Val},
      recover_log(Dc, Receiver, NewCont, Idx);
    eof ->
      MaxIndex
  end.


add_log_entries(State, Dc, Queue, MaxIdx, NewRequests) ->
  case priority_queue:peek(Queue) of
    {value, {Entry = {Idx, _}, From}} when Idx == (MaxIdx + 1) ->
      Queue2 = priority_queue:drop(Queue),
      {Log, State2} = get_log(Dc, State),
      ok = disk_log:log(Log, Entry),
      add_log_entries(State2, Dc, Queue2, Idx, [{Dc, From} | NewRequests]);
    Other ->
      State#state{
        open_requests      = NewRequests ++ State#state.open_requests,
        open_request_count = length(NewRequests) + State#state.open_request_count,
        max_index          = maps:put(Dc, MaxIdx, State#state.max_index),
        waiting            = maps:put(Dc, Queue, State#state.waiting)
      }
  end.