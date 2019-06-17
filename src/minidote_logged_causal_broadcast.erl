-module(minidote_logged_causal_broadcast).

-behavior(gen_server).

-export([start_link/2, broadcast/2, stop/1, this_node/1]).

% gen_server callbacks
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).


-define(HEARTBEAT_INTERVAL, 1500).
-define(REQUEST_INTERVAL, 1000).

-type log_entry_val() :: {Vc :: vectorclock:vectorclock(), Msg :: any()}.
-type log_entries() :: priority_queue:pq({pos_integer(), log_entry_val()}).
-record(state, {
  self :: atom(), % own node in link layer
  ll :: pid(), % link layer
  log_server :: pid(), % log server
  respond_to :: pid(),
  pending = maps:new() :: maps:map(node(), log_entries()), % pending messages per node of origin
  % current snapshot at this node
  vc = vectorclock:new() :: vectorclock:vectorclock(),
  % what is known about remote data centers (lower bound on the merge of all vectorclocks at all nodes)
  remote_vc = vectorclock:new() :: vectorclock:vectorclock(),
  % for each node: when was the last time (using erlang:monotonic_time(millisecond)), we requested to get updates?
  last_requested = maps:new() :: maps:map(node(), integer())
}).

%% messages
-record(broadcast, {message}).
-record(deliver, {message}).
-record(send_heartbeat, {}).
-record(remote_broadcast, {node :: node(), remoteVc :: vectorclock:vectorclock(), logEntries :: log_entries()}).
-record(remote_request, {requester :: pid(), node :: node(), startingFrom :: pos_integer()}).





-spec start_link(pid(), atom()) -> {ok, pid()} | ignore | {error, Error :: any()}.
start_link(RespondTo, ServerName) ->
  gen_server:start_link(?MODULE, [RespondTo, ServerName], []).

stop(Pid) ->
  gen_server:stop(Pid).


-spec broadcast(pid(), any()) -> ok.
broadcast(B, Msg) ->
  gen_server:call(B, #broadcast{message = Msg}, 2000).

this_node(B) ->
  gen_server:call(B, this_node).


init([R, ServerName]) ->
  gen_server:cast(self(), init),
  {ok, LogServer} = minidote_op_log:start_link(ServerName, self()),
  {ok, LL} = link_layer_distr_erl:start_link(minidote),
  start_heartbeat_timer(),
  link_layer:register(LL, self()),
  {ok, Self} = link_layer:this_node(LL),
  InitialState = #state{
    self       = Self,
    ll         = LL,
    respond_to = R,
    log_server = LogServer
  },
  {ok, InitialState}.


handle_call(this_node, _From, State) ->
  Node = link_layer:this_node(State#state.ll),
  {reply, Node, State};
handle_call(#broadcast{message = Msg}, From, State) ->
  % increment vectorclock
  NewVc = vectorclock:increment(State#state.vc, State#state.self),
  % broadcast to other nodes
  {ok, OtherNodes} = link_layer:other_nodes(State#state.ll),
  LogIndex = vectorclock:get(State#state.vc, State#state.self),
  LogEntry = {LogIndex, {State#state.vc, Msg}},
  NewState = State#state{
    vc = NewVc
  },
  log_to_disk_and_send_async(State#state.log_server, State#state.self, LogEntry, From, State#state.ll, State#state.self, NewVc, OtherNodes),
  {noreply, NewState}.


handle_info(#send_heartbeat{}, State) ->
  {ok, OtherNodes} = link_layer:other_nodes(State#state.ll),
  [link_layer:send(State#state.ll, #remote_broadcast{node = State#state.self, remoteVc = State#state.vc, logEntries = priority_queue:new()}, N) || N <- OtherNodes],
  start_heartbeat_timer(),
  {noreply, State};
handle_info(#remote_broadcast{node = Node, remoteVc = RemoteVc, logEntries = LogEntries}, State) ->
  NodePending = priority_queue:union(LogEntries, maps:get(Node, State#state.pending, priority_queue:new())),
  Pending = maps:put(Node, NodePending, State#state.pending),
  {NewPending, NewVc} = deliver_pending(State, Pending, State#state.vc, true),
  NewRemoteVc = vectorclock:merge(State#state.remote_vc, RemoteVc),
  NewState = request_missing(State#state{pending = NewPending, vc = NewVc, remote_vc = NewRemoteVc}),
  {noreply, NewState};
handle_info(#remote_request{requester = Requester, node = Node, startingFrom = StartingFrom}, State) ->
  LL = State#state.ll,
  Vc = State#state.vc,
  spawn_link(fun() ->
    {ok, Entries} = minidote_op_log:read_log_entries(State#state.log_server, Node, StartingFrom, all, fun(X, Xs) ->
      [X | Xs] end, []),
    link_layer:send(LL, #remote_broadcast{node = Node, remoteVc = Vc, logEntries = priority_queue:from_list(Entries)}, Requester)
  end),
  {noreply, State}.

%% deliver all eligible pending messages in causal order
-spec deliver_pending(#state{}, maps:map(node(), log_entries()), vectorclock:vectorclock(), boolean()) -> {maps:map(node(), log_entries()), vectorclock:vectorclock()}.
deliver_pending(State, Pending, Vc, LogToDisk) ->
  [case priority_queue:is_queue(Entries) of
    true -> ok;
    false ->
      throw({not_a_queue, Node, Entries})
  end || {Node, Entries} <- maps:to_list(Pending)],

  CanDeliver = [{Node, Entry} || {Node, Entries} <- maps:to_list(Pending), {_, {VcQ, _}} = Entry <- priority_queue:peek_list(Entries), vectorclock:leq(VcQ, Vc)],
  case CanDeliver of
    [] ->
      {Pending, Vc};
    _ ->
      {NewPending, NewVc} = lists:foldl(fun({Node, {_Index, {VcQ, Msg}} = Entry}, {P, V}) ->
        % remove from pending map:
        P2 = maps:update_with(Node, fun priority_queue:drop/1, P),
        case vectorclock:get(VcQ, Node) < vectorclock:get(V, Node) of
          true ->
            % already delivered, ignore
            {P2, V};
          false ->
            case LogToDisk of
              true ->
                % log to disk:
                log_to_disk_async(State#state.log_server, Node, Entry);
              false ->
                ok
            end,
            % deliver message:
            State#state.respond_to ! {deliver, Msg},
            % include in vectorclock
            V2 = vectorclock:increment(V, Node),
            {P2, V2}
        end
      end, {Pending, Vc}, CanDeliver),
      deliver_pending(State, NewPending, NewVc, LogToDisk)
  end.

%% If we are missing dependencies, request them from some other node
request_missing(State) ->
  Vc = State#state.vc,
  NodesWithMissingRequests = [N || {N, V} <- vectorclock:to_list(State#state.remote_vc), V > vectorclock:get(Vc, N)],
  Now = erlang:monotonic_time(millisecond),

  UpdatedEntries = lists:filter(fun(Node) ->
    DoRequest = case maps:find(Node, State#state.last_requested) of
      error -> true;
      {ok, LastRequest} when LastRequest + ?REQUEST_INTERVAL < Now -> true;
      _ -> false
    end,
    case DoRequest of
      false ->
        false;
      true ->
        case link_layer:other_nodes(State#state.ll) of
          {ok, []} ->
            lager:warning("No other nodes found"),
            false;
          {ok, OtherNodes} ->
            {ok, Self} = link_layer:this_node(State#state.ll),
            lists:foreach(fun(OtherNode) ->
              link_layer:send(State#state.ll, #remote_request{requester = Self, node = Node, startingFrom = vectorclock:get(Vc, Node)}, OtherNode)
            end, OtherNodes),
            true
        end
    end
  end, NodesWithMissingRequests),
  State#state{
    %% update last_requested time for updated entries
    last_requested = maps:merge(State#state.last_requested, maps:from_list([{N, Now} || N <- UpdatedEntries]))
  }.

handle_cast(init, State) ->
  {noreply, handle_recover(State)};
handle_cast(_Request, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  minidote_op_log:stop(State#state.log_server),
  link_layer:stop(State#state.ll),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

log_to_disk_async(LogServer, Node, Entry) ->
  spawn_link(fun() ->
    % log to disk and wait until it is stored persistently
    ok = minidote_op_log:add_log_entry(LogServer, Node, Entry)
  end).

log_to_disk_and_send_async(LogServer, Node, LogEntry, From, LL, Self, NewVc, OtherNodes) ->
  %spawn_link(fun() -> TODO make this async?
    % log to disk and wait until it is stored persistently
    ok = minidote_op_log:add_log_entry(LogServer, Node, LogEntry),
    % then send to other nodes and reply
    [link_layer:send(LL, #remote_broadcast{node = Self, remoteVc = NewVc, logEntries = priority_queue:singleton(LogEntry)}, N) || N <- OtherNodes],
    gen_server:reply(From, ok).
  %end).



start_heartbeat_timer() ->
  erlang:send_after(?HEARTBEAT_INTERVAL, self(), #send_heartbeat{}).

handle_recover(State) ->
  handle_recover(State, #{}).
handle_recover(State, Pending) ->
  receive
    log_recovery_done ->
      State#state.respond_to ! log_recovery_done,
      State;
    {log_recovery, Dc, Data} ->
      CurrentQueue = maps:get(Dc, Pending, priority_queue:new()),
      NewQueue = priority_queue:in(Data, CurrentQueue),
      NewPending1 = maps:put(Dc, NewQueue, Pending),
      {NewPending2, NewVc} = deliver_pending(State, NewPending1, State#state.vc, false),
      NewState = State#state{vc = NewVc},
      handle_recover(NewState, NewPending2)
  after 5000 ->
    throw({could_not_recover_from_log, timeout})
  end.