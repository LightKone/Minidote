-define(APP, minidote).

%% data types
-type message() :: term().
-type id() :: term().
-type dot() :: dot:d().
-type context() :: context:ctxt().
-type depdot() :: {dot(), context()}.