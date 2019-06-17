-module(link_layer_distr_erl).
% An implementation of the link layer using distributed Erlang
% and the pg2 process group library

-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2, code_change/3, handle_info/2]).

-record(state, {group_name, respond_to}).

start_link(GroupName) when is_atom(GroupName) ->
  gen_server:start_link(?MODULE, [GroupName], []).

stop(Pid) ->
  gen_server:stop(Pid).

init([GroupName]) ->
  % initially, try to connect with other erlang nodes
  spawn_link(fun find_other_nodes/0),
  pg2:create(GroupName),
  register(GroupName, self()),
  ok = pg2:join(GroupName, self()),
  {ok, #state{group_name = GroupName, respond_to = none}}.

terminate(_Reason, State) ->
  pg2:leave(State#state.group_name, self()).

handle_call({send, Data, Node}, _From, State) ->
  gen_server:cast({State#state.group_name, Node}, {remote, Data}),
  {reply, ok, State};
handle_call({register, R}, _From, State) ->
  {reply, ok, State#state{respond_to = R}};
handle_call(all_nodes, _From, State) ->
  Members = pg2:get_members(State#state.group_name),
  {reply, {ok, [node(M) || M <- Members]}, State};
handle_call(other_nodes, _From, State) ->
  Members = pg2:get_members(State#state.group_name),
  Self = self(),
  OtherMembers = [node(M) || M <- Members, M /= Self],
  {reply, {ok, OtherMembers}, State};
handle_call(this_node, _From, State) ->
  {reply, {ok, node()}, State}.

handle_cast({remote, Msg}, State) ->
  RespondTo = State#state.respond_to,
  RespondTo ! Msg,
  {noreply, State}.

handle_info(_Msg, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% connect to other erlang nodes using the environment variable MINIDOTE_NODES
find_other_nodes() ->
  Nodes = string:tokens(os:getenv("MINIDOTE_NODES", ""), ","),
  Nodes2 = [list_to_atom(N) || N <- Nodes],
  lager:info("Connecting ~p to other nodes: ~p", [node(), Nodes2]),
  try_connect(Nodes2, 500).


try_connect(Nodes, T) ->
  {Ping, Pong} = lists:partition(fun(N) -> pong == net_adm:ping(N) end, Nodes),
  [lager:info("Connected to node ~p", [N]) || N <- Ping],
  case T > 1000 of
    true -> [lager:info("Failed to connect ~p to node ~p", [node(), N]) || N <- Pong];
    _ -> ok
  end,
  case Pong of
    [] ->
      lager:info("Connected to all nodes");
    _ ->
      timer:sleep(T),
      try_connect(Pong, 2 * T)
  end.