%%%-------------------------------------------------------------------
%% @doc mq receiver Server
%% @end
%%%-------------------------------------------------------------------

-module(game_mq_receiver_handler).
-author("ninggq").

-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {
  handler = undefined :: atom(),
  queue = undefined :: string(),
  arg = undefined :: term(),
  connection = undefined :: pid(),
  channel = undefined :: pid(),
  ref = undefined :: reference(),
  tag = undefined :: term(),
  auto_ref = undefined :: reference()
}).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(term()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([Queue, Args, Hnadler]) ->
  erlang:process_flag(trap_exit, true),
  State = rabbitmq_connect(#state{handler = Hnadler, queue = Queue, arg = Args}),
  {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
  State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(reconnect, State) ->
  NewState = rabbitmq_connect(State),
  {noreply, NewState};
handle_info({'DOWN', Ref, process, _pid, _reason}, #state{ref = Ref} = State) ->
  NewState = rabbitmq_connect(State),
  {noreply, NewState};
handle_info(Info, #state{handler = Handler, queue = Queue, channel = Channel} = State) ->
  case mq_client_receive:handle(Info) of
    {msg, Content, #{
      exchange := Exchange,
      delivery_tag := DeliveryTag, 
      reply_to := ReplyTo, 
      correlation_id := CorrelationId
    } = Meta} ->
      mq_client:ack(Channel, DeliveryTag),
      case Handler:consume(Queue, Content, Meta) of
        ok -> 
          ok;
        {reply, Reply} ->
          ok = mq_client:sync_send(Channel, Reply, [
            {delivery_mode, 1}, 
            {exchange, Exchange},
            {routing_key, ReplyTo},
            {correlation_id, CorrelationId}
          ]),
          ok
      end;
    _ ->
      skip
  end,
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
  State :: #state{}) -> term()).
terminate(_Reason, #state{connection = Conn, channel = Channel} = _State) ->
  mq_client:stop(Conn, Channel),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
  Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
rabbitmq_connect(#state{queue = Queue, arg = Arg, auto_ref = OldTimer} = State) ->
  case mq_client:connect(Arg) of
    {ok, Conn} ->
      Ref = erlang:monitor(process, Conn),
      {ok, Channel} = mq_client:start_channel(Conn),
      {ok, _} = mq_client:declare_queue(Channel, [{queue, Queue}, {durable, true}]),
      {ok, Tag} = mq_client:subscribe(Channel, self(), [{queue, Queue}]),
      State#state{connection = Conn, channel = Channel, ref = Ref, tag = Tag};
    {error, _Error} ->
      Timer = reset_timer(OldTimer),
      State#state{auto_ref = Timer}
  end.

reset_timer(Timer) when is_reference(Timer) ->
  erlang:cancel_timer(Timer),
  erlang:send_after(10000, self(), reconnect);
reset_timer(_) ->
  erlang:send_after(10000, self(), reconnect).
