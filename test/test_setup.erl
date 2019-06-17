-module(test_setup).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% API
-export([start_slaves/2, stop_slaves/1, eventually/1, eventually/3, mock_link_layer/2, add_delay_r/3, eventually/2, counter/0, counter_get/1, counter_inc/2]).


start_slaves(Config, NodeConfig) ->
  Slaves = list_utils:pmap(fun(N) -> start_slave(N, Config) end, NodeConfig),
  [{_, FirstPort}|_] = NodeConfig,
  {ok, Pid} = antidotec_pb_socket:start("localhost", FirstPort),
  Disconnected = antidotec_pb_socket:stop(Pid),
  ?assertMatch(ok, Disconnected),
  [{nodes, Slaves}, {node_config, NodeConfig} | Config].

start_slave({Node, Port}, Config) ->
  ErlFlags =
    "-pa " ++ string:join(code:get_path(), " ") ++ " ",
  %PrivDir = proplists:get_value(priv_dir, Config),
  PrivDir = ".",
  NodeDir = filename:join([PrivDir, Node]),
  CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
  case ct_slave:start(Node,
    [{kill_if_fail, true},
      {monitor_master, true},
      {init_timeout, 10000},
      {startup_timeout, 10000},
      {env, [
        {"MINIDOTE_PORT", integer_to_list(Port)},
        {"LOG_DIR", NodeDir},
        {"OP_LOG_DIR", filename:join([NodeDir, "op_log"])}
        ]},
      {startup_functions,
        [{code, set_path, [CodePath]}]},
      {erl_flags, ErlFlags}]) of
    {ok, HostNode} ->
      ct:pal("Node ~p [OK1]", [HostNode]),
      rpc:call(HostNode, application, ensure_all_started, [minidote]),
      ct:pal("Node ~p [OK2]", [HostNode]),
      pong = net_adm:ping(HostNode),
      HostNode;
    {error, started_not_connected, HostNode} ->
      ct:pal("Node ~p [START failed] started_not_connected", [HostNode]),
      rpc:call(HostNode, application, ensure_all_started, [minidote]),
      ct:pal("Node ~p [OK2]", [HostNode]),
      pong = net_adm:ping(HostNode),
      HostNode;
    {error, already_started, HostNode} ->
      ct:pal("Node ~p [START failed] already_started", [HostNode]),
      % try again:
      stop_slave(Node),
      start_slave({Node, Port}, Config)
  end.

stop_slaves(Config) ->
  NodeConfig = proplists:get_value(node_config, Config),
  list_utils:pmap(fun({Node, _}) -> stop_slave(Node) end, NodeConfig),
  ok.

stop_slave(Node) ->
  case ct_slave:stop(Node) of
    {ok, HostNode} ->
      ct:pal("Node ~p [STOP OK]", [HostNode]);
    {error, not_started, Name} ->
      ct:pal("Node ~p [STOP FAILED] not started", [Name]);
    {error, not_connected, Name} ->
      ct:pal("Node ~p [STOP FAILED] not connected", [Name]);
    {error, stop_timeout, Name} ->
      ct:pal("Node ~p [STOP FAILED] stop_timeout", [Name])
  end.

% eventually (max. after 10 seconds)
eventually(F) ->
  eventually(10000, F).

eventually(Time, F) ->
  eventually(Time, 25, F).

eventually(Time, DT, F) ->
  try
    F()
  catch
    E:Msg when Time >= 0->
      ct:pal("Condition not yet true: ~p~n~p~n~p~nRetrying in ~pms", [E, Msg, erlang:get_stacktrace(), DT]),
      timer:sleep(DT),
      eventually(Time - DT, 2*DT, F)
  end.

% Mocks the link layer module with the given options.
% The Options parameter is a map which can have the following entries:
% - delay => true
%     This will print debug messages for which messages are sent on the link-layer
% -
-spec mock_link_layer([node()], Options) -> ok
  when Options :: #{
      delay => non_neg_integer() | fun((From::node(), To::node()) -> non_neg_integer()),
      debug => boolean()}.
mock_link_layer(Nodes, Options) ->
  [rpc:cast(Node, ?MODULE, add_delay_r, [self(), node(), Options]) || Node <- Nodes],
  [receive {add_delay_r_done, Node} -> ok end || Node <- Nodes],
  ok.

add_delay_r(Requester, ThisNode, Options) ->
  application:ensure_all_started(meck),
  ok = meck:new(link_layer, [passthrough]),
  ok = meck:expect(link_layer, send, fun(LL, Data, Receiver) ->
    spawn(fun() ->
      case maps:find(delay, Options) of
        {ok, Ms} when is_number(Ms) -> timer:sleep(Ms);
        {ok, F} when is_function(F) ->
          Ms = F(node(), Receiver),
          timer:sleep(Ms);
        error -> ok
      end,
      case maps:get(debug, Options, false) of
        true ->
          rpc:call(ThisNode, ct, pal, ["Sending message from ~p to ~p:~n ~180p", [node(), Receiver, Data]]);
        false ->
          ok
      end,
      apply(meck_util:original_name(link_layer), send, [LL, Data, Receiver])
    end)
  end),
  % answer to requester
  Requester ! {add_delay_r_done, node()},
  % wait until monitor is done:
  MonitorRef = monitor(process, Requester),
  receive
    {'DOWN', MonitorRef, _Type, _Object, _Info} ->
      ok
  end.


counter() ->
  spawn_link(fun() -> counter(0) end).

counter(N) ->
  receive
    {get, Sender} ->
      Sender ! {self(), N},
      counter(N);
    {increment, X} ->
      counter(N+X)
  end.

counter_get(C) ->
  C ! {get, self()},
  receive
    {C, N} -> N
  end.

counter_inc(C, N) ->
  C ! {increment, N}.

