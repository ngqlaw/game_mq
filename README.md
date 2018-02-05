game_mq
=====

An game rabbit mq server application

Build
-----

    $ rebar3 compile

Usage
-----
game_mq:start_sender/2 启动发送进程,
发送消息调用 game_mq:send/2
```
%% 发送消息
-spec (send(term(), binary()) -> boolean()).
```

game_mq:start_receiver/4 启动接收进程，需要传入回调模块。
回调模块要求有函数consumer/3:
```
-callback (consume(Queue :: term(), Payload :: binary(), Meta :: map()) -> ok | {reply, binary()}).
```