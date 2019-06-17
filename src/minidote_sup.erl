-module(minidote_sup).
% minidote top level supervisor.

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
  % the system consists of two main components:
  % The minidote_server which implements the actual logic
  % and the protocol buffer interface, which manages network connections.
  % We use the one_for_one strategy, because both subsystems
  % should be able to operate independently (the pb interface only sends
  % individual requests to the minidote_server, no session state is kept
  % between the two)
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
  PbSup = #{id => minidote_pb_sup,
    start => {minidote_pb_sup, start_link, []},
    restart => permanent,
    shutdown => 5000,
    type => supervisor,
    modules => [minidote_pb_sup]},
  Server = #{id => minidote_server,
      start => {minidote_server, start_link, [minidote_server]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [minidote_server]},
  {ok, {SupFlags, [
    PbSup, Server]}}.

