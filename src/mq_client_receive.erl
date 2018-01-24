%%%-------------------------------------------------------------------
%% @doc mq client receive handler API
%% @end
%%%-------------------------------------------------------------------

-module(mq_client_receive).

-include_lib ("amqp_client/include/amqp_client.hrl").

-export([
  handle/1
]).

%% 处理mq信息(转换成本地消息)
-spec (handle(term()) -> {consume_ok | cancel_ok, term()} | {cancel | msg, term(), term()} | unknown). 
handle(#'basic.consume_ok'{consumer_tag = ConsumerTag}) ->
  {consume_ok, ConsumerTag};
handle(#'basic.cancel_ok'{consumer_tag = ConsumerTag}) ->
  {cancel_ok, ConsumerTag};
handle(#'basic.cancel'{consumer_tag = ConsumerTag, nowait = Nowait}) ->
  {cancel, ConsumerTag, Nowait};
handle({#'basic.deliver'{
    consumer_tag = ConsumerTag,
    delivery_tag = DeliveryTag,
    redelivered = Redelivered, 
    exchange = Exchange, 
    routing_key = RoutingKey
  }, #amqp_msg{
    props = #'P_basic'{
      content_type = ContentType, 
      content_encoding = ContentEncoding, 
      headers = Headers, 
      delivery_mode = DeliveryMode, 
      priority = Priority,
      correlation_id = CorrelationId,
      reply_to = ReplyTo,
      expiration = Expiration,
      message_id = MessageId,
      timestamp = Timestamp,
      type = Type,
      user_id = UserId,
      app_id = AppId,
      cluster_id = ClusterId
    },
    payload = Payload
  }}) ->
  Meta = #{
    consumer_tag => ConsumerTag,
    delivery_tag => DeliveryTag,
    redelivered => Redelivered, 
    exchange => Exchange, 
    routing_key => RoutingKey,
    content_type => ContentType, 
    content_encoding => ContentEncoding, 
    headers => Headers, 
    delivery_mode => DeliveryMode, 
    priority => Priority,
    correlation_id => CorrelationId,
    reply_to => ReplyTo,
    expiration => Expiration,
    message_id => MessageId,
    timestamp => Timestamp,
    type => Type,
    user_id => UserId,
    app_id => AppId,
    cluster_id => ClusterId
  },
  {msg, Payload, Meta};
handle(_Info) ->
  unknown.
