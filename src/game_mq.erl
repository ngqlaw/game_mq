%%%-------------------------------------------------------------------
%% @doc game mq API
%% @end
%%%-------------------------------------------------------------------

-module(game_mq).

-type (start_res() :: {ok, pid()} | {ok, pid(), Info :: term()} 
| {error, already_present 
| {already_started, pid()} | term()}).

%% 通信消息处理
-callback (consume(Queue :: term(), Payload :: binary(), Meta :: map()) -> ok).

-export([
  start_sender/2
  ,start_receiver/4
  ,send/2
  ,stop/1
]).
-export([consume/3]).

%% 启动发送进程
-spec (start_sender(term(), list()) -> start_res()).
start_sender(Queue, Opt) ->
  NewOpt = normalize_options(lists:ukeysort(1, Opt ++ [
    {host, "localhost"},
    {port, undefined},
    {username, <<"guest">>},
    {password, <<"guest">>},
    {virtual_host, <<"/">>}
  ]), []),
  game_mq_sup:start_child({sender, Queue}, [NewOpt]).

%% 启动接收进程
-spec (start_receiver(term(), term(), list(), atom()) -> start_res()).
start_receiver(Queue, Sub, Opt, Handler) ->
  NewOpt = normalize_options(lists:ukeysort(1, Opt ++ [
    {host, "localhost"},
    {port, undefined},
    {username, <<"guest">>},
    {password, <<"guest">>},
    {virtual_host, <<"/">>}
  ]), []),
  game_mq_sup:start_child({consumer, {Queue, Sub}}, [NewOpt, Handler]).


%% 发送消息
-spec (send(term(), binary()) -> boolean()).
send(Queue, Msg) ->
  case game_mq_sup:get_sender(Queue) of
    undefined -> false;
    Pid -> 
      gen_server:cast(Pid, {send, Msg}),
      true
  end.

%% 停止服务
-spec (stop(term()) -> ok).
stop(Queue) ->
  game_mq_sup:stop(Queue),
  ok.

%% 测试
consume(Queue, Info, Meta) ->
  lager:info("[~p]handle msg ~p:~p", [Queue, Info, Meta]).

%%====================================================================
%% Internal functions
%%====================================================================
%% 过滤标准参数
normalize_options([{username, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{password, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{virtual_host, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{host, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{port, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{channel_max, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{frame_max, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{heartbeat, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{connection_timeout, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{ssl_options, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{auth_mechanisms, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{client_properties, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([{socket_options, _} = H|T], Res) ->
  normalize_options(T, [H|Res]);
normalize_options([_|T], Res) -> 
  normalize_options(T, Res);
normalize_options([], Res) -> lists:reverse(Res).
