%%%-------------------------------------------------------------------
%% @doc mq client API
%% @end
%%%-------------------------------------------------------------------

-module(mq_client).

-include_lib ("amqp_client/include/amqp_client.hrl").

-export([
  connect/1
  ,start_channel/1
  ,declare_exchange/2
  ,declare_queue/2
  ,send/3
  ,subscribe/3
  ,ack/2
  ,stop/2
]).

%% 启动连接
-spec (connect(Opt :: list()) -> {ok, Connection :: pid()} | {error, term()}).
connect(Opt) when is_list(Opt) ->
  Params = normalize_connect_options(Opt, #amqp_params_network{}),
  amqp_connection:start(Params).

%% 建立通道
-spec (start_channel(Connect :: pid()) -> {ok, Channel :: pid()} | {error, term()}).
start_channel(Connect) when is_pid(Connect) ->
  amqp_connection:open_channel(Connect).

%% 设置路由
-spec (declare_exchange(Channel :: pid(), Opt :: list()) -> ok | {error, term()}).
declare_exchange(Channel, Opt) when is_pid(Channel) andalso is_list(Opt) ->
  Declare = normalize_declare_exchange_options(Opt, #'exchange.declare'{}),
  case amqp_channel:call(Channel, Declare) of
    #'exchange.declare_ok'{} -> ok;
    Error -> {error, Error}
  end.

%% 设置队列
-spec (declare_queue(Channel :: pid(), Opt :: list()) -> {ok, map()} | {error, term()}).
declare_queue(Channel, Opt) when is_pid(Channel) andalso is_list(Opt) ->
  Declare = normalize_declare_queue_options(Opt, #'queue.declare'{}),
  case amqp_channel:call(Channel, Declare) of
    #'queue.declare_ok'{
      queue = Queue, 
      message_count = MessageCount, 
      consumer_count = ConsumerCount
    } ->
      {ok, #{
        queue => Queue, 
        message_count => MessageCount, 
        consumer_count => ConsumerCount}
      };
    Error -> {error, Error}
  end.

%% 发送消息
-spec (send(Channel :: pid(), Payload :: binary(), Opt :: list()) -> ok | term()).
send(Channel, Payload, Opt) when is_pid(Channel) andalso is_binary(Payload) andalso is_list(Opt) ->
  Publish = normalize_publish_options(Opt, #'basic.publish'{}),
  Props = normalize_publish_basic_options(Opt, #'P_basic'{}),
  Msg = #amqp_msg{props = Props, payload = Payload},
  amqp_channel:cast(Channel, Publish, Msg).

%% 订阅消息
-spec (subscribe(Channel :: pid(), Consumer :: pid(), Opt :: list()) -> {ok, term()} | {error, term()}).
subscribe(Channel, Consumer, Opt) when is_pid(Channel) andalso is_pid(Consumer) andalso is_list(Opt) ->
  Sub = normalize_subscribe_options(Opt, #'basic.consume'{}),
  case amqp_channel:subscribe(Channel, Sub, Consumer) of
    #'basic.consume_ok'{consumer_tag = Tag} -> {ok, Tag};
    Error -> {error, Error}
  end.

%% 回复
-spec (ack(Channel :: pid(), Tag :: term()) -> ok).
ack(Channel, Tag) ->
  amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}).

%% 关闭
-spec (stop(pid(), pid()) -> ok).
stop(Conn, Channel) ->
  amqp_channel:close(Channel),
  amqp_connection:close(Conn),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% 初始化连接设置
normalize_connect_options([{username, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{username = V});
normalize_connect_options([{password, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{password = V});
normalize_connect_options([{virtual_host, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{virtual_host = V});
normalize_connect_options([{host, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{host = V});
normalize_connect_options([{port, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{port = V});
normalize_connect_options([{channel_max, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{channel_max = V});
normalize_connect_options([{frame_max, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{frame_max = V});
normalize_connect_options([{heartbeat, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{heartbeat = V});
normalize_connect_options([{connection_timeout, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{connection_timeout = V});
normalize_connect_options([{ssl_options, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{ssl_options = V});
normalize_connect_options([{auth_mechanisms, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{auth_mechanisms = V});
normalize_connect_options([{client_properties, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{client_properties = V});
normalize_connect_options([{socket_options, V}|T], Params) ->
  normalize_connect_options(T, Params#amqp_params_network{socket_options = V});
normalize_connect_options([_|T], Params) -> 
  normalize_connect_options(T, Params);
normalize_connect_options([], Params) -> Params.

%% 初始化路由设置
normalize_declare_exchange_options([{ticket, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{ticket = V});
normalize_declare_exchange_options([{exchange, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{exchange = to_binary(V)});
normalize_declare_exchange_options([{type, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{type = V});
normalize_declare_exchange_options([{passive, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{passive = V});
normalize_declare_exchange_options([{durable, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{durable = V});
normalize_declare_exchange_options([{auto_delete, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{auto_delete = V});
normalize_declare_exchange_options([{internal, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{internal = V});
normalize_declare_exchange_options([{nowait, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{nowait = V});
normalize_declare_exchange_options([{arguments, V}|T], Declare) ->
  normalize_declare_exchange_options(T, Declare#'exchange.declare'{arguments = V});
normalize_declare_exchange_options([_|T], Declare) -> 
  normalize_declare_exchange_options(T, Declare);
normalize_declare_exchange_options([], Declare) -> Declare.

%% 初始化队列设置
normalize_declare_queue_options([{ticket, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{ticket = V});
normalize_declare_queue_options([{queue, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{queue = to_binary(V)});
normalize_declare_queue_options([{passive, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{passive = V});
normalize_declare_queue_options([{durable, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{durable = V});
normalize_declare_queue_options([{exclusive, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{exclusive = V});
normalize_declare_queue_options([{auto_delete, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{auto_delete = V});
normalize_declare_queue_options([{nowait, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{nowait = V});
normalize_declare_queue_options([{arguments, V}|T], Declare) ->
  normalize_declare_queue_options(T, Declare#'queue.declare'{arguments = V});
normalize_declare_queue_options([_|T], Declare) -> 
  normalize_declare_queue_options(T, Declare);
normalize_declare_queue_options([], Declare) -> Declare.

%% 初始化消息发送
normalize_publish_options([{ticket, V}|T], Publish) ->
  normalize_publish_options(T, Publish#'basic.publish'{ticket = V});
normalize_publish_options([{exchange, V}|T], Publish) ->
  normalize_publish_options(T, Publish#'basic.publish'{exchange = to_binary(V)});
normalize_publish_options([{routing_key, V}|T], Publish) ->
  normalize_publish_options(T, Publish#'basic.publish'{routing_key = to_binary(V)});
normalize_publish_options([{mandatory, V}|T], Publish) ->
  normalize_publish_options(T, Publish#'basic.publish'{mandatory = V});
normalize_publish_options([{immediate, V}|T], Publish) ->
  normalize_publish_options(T, Publish#'basic.publish'{immediate = V});
normalize_publish_options([_|T], Publish) ->
  normalize_publish_options(T, Publish);
normalize_publish_options([], Publish) -> Publish.

%% 初始化消息发送基础属性
normalize_publish_basic_options([{content_type, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{content_type = V});
normalize_publish_basic_options([{content_encoding, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{content_encoding = V});
normalize_publish_basic_options([{headers, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{headers = V});
normalize_publish_basic_options([{delivery_mode, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{delivery_mode = V});
normalize_publish_basic_options([{priority, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{priority = V});
normalize_publish_basic_options([{correlation_id, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{correlation_id = V});
normalize_publish_basic_options([{reply_to, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{reply_to = V});
normalize_publish_basic_options([{expiration, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{expiration = V});
normalize_publish_basic_options([{message_id, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{message_id = V});
normalize_publish_basic_options([{timestamp, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{timestamp = V});
normalize_publish_basic_options([{type, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{type = V});
normalize_publish_basic_options([{user_id, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{user_id = V});
normalize_publish_basic_options([{app_id, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{app_id = V});
normalize_publish_basic_options([{cluster_id, V}|T], Basic) ->
  normalize_publish_basic_options(T, Basic#'P_basic'{cluster_id = V});
normalize_publish_basic_options([_|T], Basic) ->
  normalize_publish_basic_options(T, Basic);
normalize_publish_basic_options([], Basic) -> Basic.

%% 初始化订阅
normalize_subscribe_options([{ticket, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{ticket = V});
normalize_subscribe_options([{queue, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{queue = to_binary(V)});
normalize_subscribe_options([{consumer_tag, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{consumer_tag = V});
normalize_subscribe_options([{no_local, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{no_local = V});
normalize_subscribe_options([{no_ack, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{no_ack = V});
normalize_subscribe_options([{exclusive, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{exclusive = V});
normalize_subscribe_options([{nowait, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{nowait = V});
normalize_subscribe_options([{arguments, V}|T], Consume) ->
  normalize_subscribe_options(T, Consume#'basic.consume'{arguments = V});
normalize_subscribe_options([_|T], Consume) ->
  normalize_subscribe_options(T, Consume);
normalize_subscribe_options([], Consume) -> Consume.

%% 转换binary
to_binary(A) when is_atom(A) -> atom_to_binary(A, unicode);
to_binary(L) when is_list(L) -> 
  case lists:all(fun(H) -> is_integer(H) andalso H >= 0 andalso H =< 255 end, L) of
    true -> list_to_binary(L);
    false -> term_to_binary(L)
  end;
to_binary(I) when is_integer(I) -> integer_to_binary(I);
to_binary(F) when is_float(F) -> float_to_binary(F);
to_binary(B) when is_binary(B) -> B;
to_binary(T) -> term_to_binary(T).
