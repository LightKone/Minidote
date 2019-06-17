%%%-------------------------------------------------------------------
%% @doc minidote public API
%% @end
%%%-------------------------------------------------------------------

-module(minidote_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

% This is the main entry point for starting the Minidote application
start(_StartType, _StartArgs) ->
  % set the log directory for lager:
  LogDir = os:getenv("LOG_DIR", "data/"),
  application:set_env(lager, log_root, LogDir),
  % allow up to 1000 log messages per second:
  application:set_env(lager, error_logger_hwm, 1000),
  application:set_env(lager, handlers, [
    {lager_console_backend, [{level, info}]},
    {lager_file_backend, [{file, "error.log"}, {level, error}]},
    {lager_file_backend, [{file, "console.log"}, {level, info}]}
  ]),
  application:ensure_all_started(lager),
  lager:info("Starting Minidote Application"),
  % Change the secret Erlang cookie if given as environment variable:
  change_cookie(),
  lager:info("Starting Minidote~n"),
  % Start the main Minidote supervisor
  minidote_sup:start_link().

% change cookie from environment value
change_cookie() ->
  case os:getenv("ERLANG_COOKIE") of
    false -> ok;
    Cookie ->
      try
        erlang:set_cookie(node(), list_to_atom(Cookie))
      catch
        E:Err ->
          lager:error("Could not set erlang cookie: ~p:~p", [E, Err])
      end
  end.

%%--------------------------------------------------------------------
stop(_State) ->
  ok.

%%====================================================================
%% Internal functions
%%====================================================================
