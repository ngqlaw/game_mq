game_mq
=====

An game rabbit mq server application

Build
-----

    $ rebar3 compile

Usage
-----
game_mq:start_sender/2 启动发送进程；
game_mq:start_receiver/4 启动接收进程，需要传入回调模块。

回调模块要求有函数consumer/3:
```
-callback (consume(Queue :: term(), Payload :: binary(), Meta :: map()) -> ok).
```