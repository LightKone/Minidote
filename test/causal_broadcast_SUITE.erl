-module(causal_broadcast_SUITE).

% Tests the minidote_logged_causal_broadcast module
% Uses a mock implementation for the link layer and the the real log layer

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-export([all/0, test_causality/1, test_with_crash/1, test_no_crash/1, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2, parallel_test/1]).

all() -> [test_no_crash, test_with_crash, test_causality, parallel_test].

init_per_suite(Config) ->
  Config.

init_per_testcase(TestCase, Config) ->
  % Create dummy link layer for testing:
  TestNodes = [nodeA, nodeB, nodeC],
  {ok, LLD, LLNodes} = link_layer_dummy:start_link(TestNodes),
  NextLL = spawn(fun() ->
    lists:map(fun(LL) ->
      receive
        {Sender, get_link_layer} ->
          Sender ! {self(), LL}
      end
    end, LLNodes)
  end),
  % setup mocks:
  meck:unload(),
  % link_layer_distr_erl:start_link(minidote)

  ok = meck:new(link_layer_distr_erl, [no_link]),
  ok = meck:expect(link_layer_distr_erl, start_link, fun(_) ->
    NextLL ! {self(), get_link_layer},
    receive
      {NextLL, LL} ->
        {ok, LL}
    after 1000 ->
      throw(link_layer_distr_erl_start_timeout)
    end
  end),
  [{link_layer_dummy, LLD} | Config].

end_per_testcase(TestCase, Config) ->
  true = meck:validate(link_layer_distr_erl),
  meck:unload(),
  Config.


end_per_suite(Config) ->
  Config.

% a simple chat server for testing the broadcast:
chat_server(TestName, NodeName) ->
  LogServerName = list_to_atom(lists:flatten(io_lib:format("log_server~p_~p", [TestName, NodeName]))),
  {ok, B} = minidote_logged_causal_broadcast:start_link(self(), LogServerName),
  chat_loop_init(B, []).

chat_loop_init(B, Received) ->
  receive
    {deliver, Msg} ->
      chat_loop_init(B, [Msg | Received]);
    log_recovery_done ->
      chat_loop(B, Received)
  end.

chat_loop(B, Received) ->
  receive
    {post, From, Msg} ->
      minidote_logged_causal_broadcast:broadcast(B, Msg),
      From ! {self(), ok},
      chat_loop(B, [Msg | Received]);
    {deliver, Msg} ->
      chat_loop(B, [Msg | Received]);
    {get_received, From} ->
      From ! {self(), lists:reverse(Received)},
      chat_loop(B, Received);
    Other ->
      throw({chat_loop_unhandled_message, Other})
  end.

test_no_crash(Config) ->
  LLD = proplists:get_value(link_layer_dummy, Config),

  % Create 3 chat servers:
  Chat1 = spawn_link(fun() -> chat_server(test1, chat1) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeA),
  Chat2 = spawn_link(fun() -> chat_server(test1, chat2) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeB),
  Chat3 = spawn_link(fun() -> chat_server(test1, chat3) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeC),

  % post a message to chatserver 1
  Chat1 ! {post, self(), 'Hello everyone!'},
  receive {Chat1, ok} -> ok end,

  % finish exchanging messages
  link_layer_dummy:finish(LLD),

  % check that all chat-servers got the message:
  Chat1 ! {get_received, self()},
  Chat2 ! {get_received, self()},
  Chat3 ! {get_received, self()},
  receive {Chat1, Received1} -> ok end,
  receive {Chat2, Received2} -> ok end,
  receive {Chat3, Received3} -> ok end,
  ?assertEqual(['Hello everyone!'], Received1),
  ?assertEqual(['Hello everyone!'], Received2),
  ?assertEqual(['Hello everyone!'], Received3).



test_with_crash(Config) ->
  LLD = proplists:get_value(link_layer_dummy, Config),

  % Create 3 chat servers:
  Chat1 = spawn_link(fun() -> chat_server(test2, chat1) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeA),
  Chat2 = spawn_link(fun() -> chat_server(test2, chat2) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeB),
  Chat3 = spawn_link(fun() -> chat_server(test2, chat3) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeC),

  % post a message to chatserver 1
  Chat1 ! {post, self(), 'Hi!'},
  receive {Chat1, ok} -> ok end,

  % deliver one message from nodeA to nodeB:
  link_layer_dummy:deliver(LLD, nodeA, nodeB, 0),
  % then crash nodeA -> message from nodeA to nodeC is lost
  link_layer_dummy:crash(LLD, nodeA),
  % finish sending all messages
  link_layer_dummy:finish(LLD),

  % Wait for timeouts
  test_setup:eventually(fun() ->
    link_layer_dummy:finish(LLD),
    % Chat1 and Chat2 are not crashed, so they both should have received the message:
    Chat2 ! {get_received, self()},
    Chat3 ! {get_received, self()},
    receive {Chat2, Received2} -> ok end,
    receive {Chat3, Received3} -> ok end,
    ?assertEqual(['Hi!'], Received2),
    ?assertEqual(['Hi!'], Received3)
  end).



test_causality(Config) ->
  LLD = proplists:get_value(link_layer_dummy, Config),

  % Create 3 chat servers:
  Chat1 = spawn_link(fun() -> chat_server(test3, chat1) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeA),
  Chat2 = spawn_link(fun() -> chat_server(test3, chat2) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeB),
  Chat3 = spawn_link(fun() -> chat_server(test3, chat3) end),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeC),


  % post a message to chatserver 1
  Chat1 ! {post, self(), 'How are you?'},
  receive {Chat1, ok} -> ok end,

  % deliver message to chatserver 2
  link_layer_dummy:deliver(LLD, nodeA, nodeB, 0),

  % Chat 2 should have received the message:
  test_setup:eventually(fun() ->
    Chat2 ! {get_received, self()},
    receive {Chat2, Rcv2} -> ok end,
    ?assertEqual(['How are you?'], Rcv2)
  end),

  % post a message to Chat 2
  Chat2 ! {post, self(), 'fine'},
  receive {Chat2, ok} -> ok end,

  % deliver message from 2 to 3 (skipping one message)
  link_layer_dummy:deliver(LLD, nodeB, nodeC, 1),

  % finish exchanging messages
  test_setup:eventually(fun() ->
    link_layer_dummy:finish(LLD),

    % check that all chat-servers got the message:
    Chat1 ! {get_received, self()},
    Chat2 ! {get_received, self()},
    Chat3 ! {get_received, self()},
    receive {Chat1, Received1} -> ok end,
    receive {Chat2, Received2} -> ok end,
    receive {Chat3, Received3} -> ok end,
    ?assertEqual(['How are you?', 'fine'], Received1),
    ?assertEqual(['How are you?', 'fine'], Received2),
    ?assertEqual(['How are you?', 'fine'], Received3)
  end).



parallel_test(Config) ->
  NumProcesses = 10,
  MessagesPerProcess = 100,
  LLD = proplists:get_value(link_layer_dummy, Config),

  Receiver1 = new_receiver(),
  Receiver2 = new_receiver(),
  Receiver3 = new_receiver(),

  % Create 3 chat servers:
  {ok, A} = minidote_logged_causal_broadcast:start_link(Receiver1, 'parallel_test1'),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeA),
  {ok, B} = minidote_logged_causal_broadcast:start_link(Receiver2, 'parallel_test2'),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeB),
  {ok, C} = minidote_logged_causal_broadcast:start_link(Receiver3, 'parallel_test3'),
  registered = link_layer_dummy:wait_until_registered(LLD, nodeC),

  list_utils:pmap(fun(I) ->
    [minidote_logged_causal_broadcast:broadcast(A, {msg, I, J}) || J <- lists:seq(1, MessagesPerProcess)]
  end, lists:seq(1, NumProcesses)),

  % deliver messages
  test_setup:eventually(fun() ->
    link_layer_dummy:deliver_all(LLD),

    % check that all messages are received:
    Receiver2 ! {self(), get},
    receive
      {Receiver2, Messages} ->
        ct:pal("Received:~n~p", [Messages]),
        ?assertEqual(NumProcesses * MessagesPerProcess, length(Messages))
    after 100 ->
      throw(timeout)
    end
  end).


new_receiver() ->
  spawn_link(fun() ->
    receive
      log_recovery_done ->
        ok
    after 5000 ->
      throw('new_receiver timed out: did not receive log_recovery_done')
    end,
    receive_loop([])
  end).

receive_loop(Received) ->
  receive
    {Sender, stop} ->
      Sender ! {self(), ok};
    {Sender, get} ->
      Sender ! {self(), lists:reverse(Received)},
      receive_loop(Received);
    Msg ->
      receive_loop([Msg | Received])
  end.
