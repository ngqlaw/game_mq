%%%-------------------------------------------------------------------
%% @doc game_mq top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(game_mq_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([
  start_child/2
  ,get_sender/1
  ,stop/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(CHILD(Mod, Key, Args), {{Mod, Key}, {Mod, start_link, [Args]}, permanent, 5000, worker, [Mod]}).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% 启动进程服务
-spec (start_child({consumer | sender, term()}, list()) -> 
  {ok, pid()} | {ok, pid(), Info :: term()} | {error, already_present | {already_started, pid()} | term()}).
start_child({consumer, {Queue, Sub}}, Args) ->
  Child = ?CHILD(game_mq_receiver_handler, {Queue, Sub}, [Queue|Args]),
  supervisor:start_child(?SERVER, Child);
start_child({sender, Queue}, Args) ->
  Child = ?CHILD(game_mq_sender_handler, Queue, [Queue|Args]),
  supervisor:start_child(?SERVER, Child).

%% 获取发送进程
-spec (get_sender(term()) -> pid() | undefined).
get_sender(Queue) ->
  Children = supervisor:which_children(?SERVER),
  case lists:keyfind({game_mq_sender_handler, Queue}, 1, Children) of
    {_, Pid, _, _} -> Pid;
    _ -> undefined
  end.

%% 停止服务
-spec (stop(term()) -> ok).
stop(Queue) ->
  Children = supervisor:which_children(?SERVER),
  lists:foreach(
    fun({{_, {Q, _}} = Id, _, _, _}) ->
      case Q == Queue of
        true -> 
          supervisor:terminate_child(?SERVER, Id),
          supervisor:delete_child(?SERVER, Id);
        false -> skip
      end;
    ({{_, Q} = Id, _, _, _}) ->
      case Q == Queue of
        true -> 
          supervisor:terminate_child(?SERVER, Id),
          supervisor:delete_child(?SERVER, Id);
        false -> skip
      end;
    (_) -> skip
  end, Children).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
  {ok, { {one_for_one, 3, 10}, []} }.

%%====================================================================
%% Internal functions
%%====================================================================
