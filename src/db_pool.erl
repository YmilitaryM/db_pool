%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%      Copyright (C) 2019 ... All rights reserved.
%%      FileName ：db_pool.erl
%%      Create   ：Jin <ymilitarym@163.com
%%      Date     : 2019-04-25
%%      Describle: 
%%      
%%      
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(db_pool).

-behaviour(p1_fsm).

-export([
         start/1,
         start_link/1,

         sql_query/2,
         sql_query_t/1,
         sql_transaction/2,
         escape/1,
         keep_alive/1,
         hex/1
        ]).

%% p1_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, print_state/1,
         code_change/4]).

-export([connecting/2, connecting/3,
         session_established/2, session_established/3]).


-include("logger.hrl").

-record(state,
        {db_ref = self()                     :: pid(),
         db_type = odbc                      :: pgsql | mysql | odbc,
         db_host_opts = []                   :: list(),
         start_interval = 0                  :: non_neg_integer(),
         max_pending_requests_len            :: non_neg_integer(),
         pending_requests = {0, queue:new()} :: {non_neg_integer(), queue:queue()}}).

-define(STATE_KEY, db_pool_state).

-define(NESTING_KEY, db_pool_nesting_level).

-define(TOP_LEVEL_TXN, 0).

-define(PGSQL_PORT, 5432).

-define(MYSQL_PORT, 3306).

-define(MAX_TRANSACTION_RESTARTS, 3).

-define(TRANSACTION_TIMEOUT, 5000).

-define(KEEPALIVE_TIMEOUT, 60000).

-define(KEEPALIVE_QUERY, <<"SELECT 1;">>).

%%-define(DBGFSM, true).
-ifdef(DBGFSM).
-define(FSMOPTS, [{debug, [trace]}]).
-else.
-define(FSMOPTS, []).
-endif.


%%****************************************************************************
%% API functions
%%****************************************************************************

start(Host) ->
    p1_fsm:start(db_pool, [Host],
                     fsm_limit_opts() ++ (?FSMOPTS)).

start_link(Config) ->
    p1_fsm:start_link(db_pool,
                          Config,
                          fsm_limit_opts() ++ (?FSMOPTS)).

-type sql_query() :: [sql_query() | binary()].
-type sql_query_result() :: {updated, non_neg_integer()} |
                            {error, binary()} |
                            {selected, [binary()], [[binary()]]} |
                            {selected, [any()]}.
-spec sql_query(binary(), sql_query()) -> sql_query_result().
sql_query(Host, Query) ->
    sql_call(Host, {sql_query, Query}).

-spec sql_transaction(binary(), [sql_query()] | fun(() -> any())) ->
                                   {atomic, any()} |
                                   {aborted, any()}.
sql_transaction(Host, Queries)
  when is_list(Queries) ->
    F = fun () ->
                lists:foreach(fun (Query) -> sql_query_t(Query) end,
                              Queries)
        end,
    sql_transaction(Host, F);
%% SQL transaction, based on a erlang anonymous function (F = fun)
sql_transaction(Host, F) when is_function(F) ->
    sql_call(Host, {sql_transaction, F}).


keep_alive(PID) ->
    p1_fsm:sync_send_event(PID,
                               {sql_cmd, {sql_query, ?KEEPALIVE_QUERY}, os:timestamp()},
                               ?KEEPALIVE_TIMEOUT).

%% This function is intended to be used from inside an sql_transaction:
-spec sql_query_t(sql_query()) -> sql_query_result().
sql_query_t(Query) ->
    QRes = sql_query_internal(Query),
    case QRes of
        {error, Reason} ->
            ?INFO_MSG("Reason:~p ", [Reason]),
            throw({aborted, Reason});
        Rs when is_list(Rs) ->
            case lists:keysearch(error, 1, Rs) of
                {value, {error, Reason}} -> throw({aborted, Reason});
                _ -> QRes
            end;
        _ -> QRes
    end.

%% Escape character that will confuse an SQL engine
 -spec escape(binary()) -> binary().
escape(S) ->
    <<  <<(escape_1(Char))/binary>> || <<Char>> <= S >>.

escape_1($\000) -> <<"\\0">>;
escape_1($\n) -> <<"\\n">>;
escape_1($\t) -> <<"\\t">>;
escape_1($\b) -> <<"\\b">>;
escape_1($\r) -> <<"\\r">>;
escape_1($') -> <<"''">>;
escape_1($") -> <<"\\\"">>;
escape_1($\\) -> <<"\\\\">>;
escape_1(C) -> <<C>>.

hex(L) when is_binary(L) ->
    hex(binary_to_list(L));
hex(L) when is_list (L) ->
    iolist_to_binary([hex(I) || I <- L]);
hex(I) when I > 16#f ->
    [hex0((I band 16#f0) bsr 4), hex0((I band 16#0f))];
hex(I)               ->
    [$0, hex0(I)].

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I)  -> $0 + I.

%%****************************************************************************
%% callback functions
%%****************************************************************************

init(Config) ->
    process_flag(trap_exit, true),
    Server = proplists:get_value(server, Config),
    DataBase = proplists:get_value(database, Config),
    Username = proplists:get_value(username, Config),
    Password = proplists:get_value(password, Config),
    Port = proplists:get_value(port, Config),
    StartInterval = proplists:get_value(start_interval, Config),
    KeepaliveInterval = proplists:get_value(keepalive_interval, Config),
		DBType = proplists:get_value(db_type, Config, mysql),
    timer:apply_interval(KeepaliveInterval * 1000, ?MODULE, keep_alive, [self()]),
    p1_fsm:send_event(self(), connect),
    DBHostOpts = [Server, Port, DataBase, Username, Password],
    {ok, connecting,
     #state{db_type = DBType, db_host_opts = DBHostOpts,
            max_pending_requests_len = max_queue(),
            pending_requests = {0, queue:new()},
            start_interval = StartInterval * 1000}}.

connecting(connect, #state{db_type = DBType, db_host_opts = DBHostOpts} = State) ->
    ConnectRes = case [DBType | DBHostOpts] of
                     [mysql | Args] -> apply(fun mysql_connect/5, Args);
                     [pgsql | Args] -> apply(fun pgsql_connect/5, Args);
                     [odbc | Args] -> apply(fun odbc_connect/1, Args)
                 end,
    {_, PendingRequests} = State#state.pending_requests,
    case ConnectRes of
        {ok, Ref} ->
            erlang:monitor(process, Ref),
            lists:foreach(fun (Req) ->
                                  p1_fsm:send_event(self(), Req)
                          end,
                          queue:to_list(PendingRequests)),
            {next_state, session_established,
             State#state{db_ref = Ref,
                         pending_requests = {0, queue:new()}}};
        {error, Reason} ->
            ?WARNING_MSG("~p connection failed:~n** Reason: ~p~n** "
                      "Retry after: ~p seconds",
                      [State#state.db_type, Reason,
                       State#state.start_interval div 1000]),
            p1_fsm:send_event_after(State#state.start_interval,
                                        connect),
            {next_state, connecting, State}
    end;
connecting(Event, State) ->
    ?WARNING_MSG("unexpected event in 'connecting': ~p",
                 [Event]),
    {next_state, connecting, State}.

connecting({sql_cmd, {sql_query, ?KEEPALIVE_QUERY},
            _Timestamp},
           From, State) ->
    p1_fsm:reply(From,
                     {error, <<"SQL connection failed">>}),
    {next_state, connecting, State};
connecting({sql_cmd, Command, Timestamp} = Req, From,
           State) ->
    ?DEBUG("queuing pending request while connecting:~n\t~p",
           [Req]),
    {Len, PendingRequests} = State#state.pending_requests,
    NewPendingRequests = if Len <
                            State#state.max_pending_requests_len ->
                                 {Len + 1,
                                  queue:in({sql_cmd, Command, From, Timestamp},
                                           PendingRequests)};
                            true ->
                                 lists:foreach(fun ({sql_cmd, _, To,
                                                     _Timestamp}) ->
                                                       p1_fsm:reply(To,
                                                                        {error,
                                                                         <<"SQL connection failed">>})
                                               end,
                                               queue:to_list(PendingRequests)),
                                 {1,
                                  queue:from_list([{sql_cmd, Command, From,
                                                    Timestamp}])}
                         end,
    {next_state, connecting,
     State#state{pending_requests = NewPendingRequests}};
connecting(Request, {Who, _Ref}, State) ->
    ?WARNING_MSG("unexpected call ~p from ~p in 'connecting'",
                 [Request, Who]),
    {reply, {error, badarg}, connecting, State}.

session_established({sql_cmd, Command, Timestamp}, From,
                    State) ->
    run_sql_cmd(Command, From, State, Timestamp);
session_established(Request, {Who, _Ref}, State) ->
    ?WARNING_MSG("unexpected call ~p from ~p in 'session_establ"
                 "ished'",
                 [Request, Who]),
    {reply, {error, badarg}, session_established, State}.

session_established({sql_cmd, Command, From, Timestamp},
                    State) ->
    run_sql_cmd(Command, From, State, Timestamp);
session_established(Event, State) ->
    ?WARNING_MSG("unexpected event in 'session_established': ~p",
                 [Event]),
    {next_state, session_established, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, badarg}, StateName, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% We receive the down signal when we loose the MySQL connection (we are
%% monitoring the connection)
handle_info({'DOWN', _MonitorRef, process, _Pid, _Info},
            _StateName, State) ->
    p1_fsm:send_event(self(), connect),
    {next_state, connecting, State#state{ db_ref=undefined }};
handle_info(Info, StateName, State) ->
    ?WARNING_MSG("unexpected info in ~p: ~p",
                 [StateName, Info]),
    {next_state, StateName, State}.

terminate(_Reason, _StateName, #state{} = State) ->
    case State#state.db_type of
        mysql -> catch p1_mysql_conn:stop(State#state.db_ref);
        _ -> ok
    end,
    ok.

%%----------------------------------------------------------------------
%% Func: print_state/1
%% Purpose: Prepare the state to be printed on error log
%% Returns: State to print
%%----------------------------------------------------------------------
print_state(State) -> State.


%%****************************************************************************
%% internal functions
%%****************************************************************************

sql_call(Host, Msg) ->
    SqlResult = try sql_call_1(Host, Msg) of
                    Result ->
                        Result
                catch
                    exit:{timeout,_} ->
                        {error, timeout}
                end,
    SqlResult.

sql_call_1(Host, Msg) ->
    case get(?STATE_KEY) of
        undefined ->
            case db_pool_sup:get_random_pid(Host) of
                undefined ->
                    ?ERROR_MSG("No connection to DB: Host=~p, Msg=~p~n", [Host, Msg]),
                    {error, <<"No connection to DB">>};
                Pid ->
                    p1_fsm:sync_send_event(Pid, {sql_cmd, Msg, os:timestamp()}, ?TRANSACTION_TIMEOUT)
            end;
        _State -> nested_op(Msg)
    end.

run_sql_cmd(Command, From, State, Timestamp) ->
    case timer:now_diff(os:timestamp(), Timestamp) div 1000 of
        Age when Age < (?TRANSACTION_TIMEOUT) ->
            put(?NESTING_KEY, ?TOP_LEVEL_TXN),
            put(?STATE_KEY, State),
            abort_on_driver_error(outer_op(Command), From);
        Age ->
            ?WARNING_MSG("Database was not available or too slow, "
                         "discarding ~p milliseconds old request~n~p~n",
                         [Age, Command]),
                                                %log_failed_sql(run_sql_cmd, Command),
            {next_state, session_established, State}
    end.

%% Only called by handle_call, only handles top level operations.
%% @spec outer_op(Op) -> {error, Reason} | {aborted, Reason} | {atomic, Result}
outer_op({sql_query, Query}) ->
    sql_query_internal(Query);
outer_op({sql_transaction, F}) ->
    outer_transaction(F, ?MAX_TRANSACTION_RESTARTS, <<"">>);
outer_op({sql_bloc, F}) -> execute_bloc(F).

%% Called via sql_query/transaction/bloc from client code when inside a
%% nested operation
nested_op({sql_query, Query}) ->
    sql_query_internal(Query);
nested_op({sql_transaction, F}) ->
    NestingLevel = get(?NESTING_KEY),
    if NestingLevel =:= (?TOP_LEVEL_TXN) ->
            outer_transaction(F, ?MAX_TRANSACTION_RESTARTS, <<"">>);
       true -> inner_transaction(F)
    end;
nested_op({sql_bloc, F}) -> execute_bloc(F).

%% Never retry nested transactions - only outer transactions
inner_transaction(F) ->
    PreviousNestingLevel = get(?NESTING_KEY),
    case get(?NESTING_KEY) of
        ?TOP_LEVEL_TXN ->
            {backtrace, T} = process_info(self(), backtrace),
            ?ERROR_MSG("inner transaction called at outer txn "
                       "level. Trace: ~s",
                       [T]),
            erlang:exit(implementation_faulty);
        _N -> ok
    end,
    put(?NESTING_KEY, PreviousNestingLevel + 1),
    Result = (catch F()),
    put(?NESTING_KEY, PreviousNestingLevel),
    case Result of
        {aborted, Reason} -> {aborted, Reason};
        {'EXIT', Reason} -> {'EXIT', Reason};
        {atomic, Res} -> {atomic, Res};
        Res -> {atomic, Res}
    end.

outer_transaction(F, NRestarts, _Reason) ->
    PreviousNestingLevel = get(?NESTING_KEY),
    case get(?NESTING_KEY) of
        ?TOP_LEVEL_TXN -> ok;
        _N ->
            {backtrace, T} = process_info(self(), backtrace),
            ?ERROR_MSG("outer transaction called at inner txn "
                       "level. Trace: ~s",
                       [T]),
            erlang:exit(implementation_faulty)
    end,
    sql_query_internal(<<"begin;">>),
    put(?NESTING_KEY, PreviousNestingLevel + 1),
    Result = (catch F()),
    put(?NESTING_KEY, PreviousNestingLevel),
    case Result of
        {aborted, Reason} when NRestarts > 0 ->
            sql_query_internal(<<"rollback;">>),
            outer_transaction(F, NRestarts - 1, Reason);
        {aborted, Reason, StackTrace} when NRestarts =:= 0 ->
            ?ERROR_MSG("SQL transaction restarts exceeded~n** "
                       "Restarts: ~p~n** Last abort reason: "
                       "~p~n** Stacktrace: ~p~n** When State "
                       "== ~p",
                       [?MAX_TRANSACTION_RESTARTS, Reason,
                        StackTrace, get(?STATE_KEY)]),
            sql_query_internal(<<"rollback;">>),
            {aborted, Reason};
        {'EXIT', Reason, StackTrace} ->
            ?ERROR_MSG("SQL transaction error~n** "
                       "Last abort reason: "
                       "~p~n** Stacktrace: ~p~n** When State "
                       "== ~p",
                       [Reason,
                        StackTrace, get(?STATE_KEY)]),
            sql_query_internal(<<"rollback;">>), {aborted, Reason};
        Res -> sql_query_internal(<<"commit;">>), {atomic, Res}
    end.

execute_bloc(F) ->
    case catch F() of
        {aborted, Reason} -> {aborted, Reason};
        {'EXIT', Reason} -> {aborted, Reason};
        Res -> {atomic, Res}
    end.

sql_query_internal(Query) ->
    State = get(?STATE_KEY),
    Res = case State#state.db_type of
              odbc ->
                  to_odbc(odbc:sql_query(State#state.db_ref, Query,
                                         (?TRANSACTION_TIMEOUT) - 1000));
              pgsql ->
                  pgsql_to_odbc(pgsql:squery(State#state.db_ref, Query));
              mysql ->
                  case Query of
                      <<"SELECT 1;">> -> ok;
                      _ -> ?DEBUG("MySQL, Send query~n~p~n", [Query])
                  end,
                  %%squery to be able to specify result_type = binary
                  %%[Query] because p1_mysql_conn expect query to be a list (elements can be binaries, or iolist)
                  %%        but doesn't accept just a binary
                  R = mysql_to_odbc(p1_mysql_conn:squery(State#state.db_ref,
                                                         [Query], self(),
                                                         [{timeout, (?TRANSACTION_TIMEOUT) - 1000},
                                                          {result_type, binary}])),
                  %% ?INFO_MSG("MySQL, Received result~n~p~n", [R]),
                  R
          end,
    case Res of
        {error, <<"No SQL-driver information available.">>} ->
            {updated, 0};
        _Else -> Res
    end.

%% Generate the OTP callback return tuple depending on the driver result.
abort_on_driver_error({error, <<"query timed out">>} =
                          Reply,
                      From) ->
    p1_fsm:reply(From, Reply),
    {stop, timeout, get(?STATE_KEY)};
abort_on_driver_error({error,
                       <<"Failed sending data on socket", _/binary>>} =
                          Reply,
                      From) ->
    p1_fsm:reply(From, Reply),
    {stop, closed, get(?STATE_KEY)};
abort_on_driver_error(Reply, From) ->
    p1_fsm:reply(From, Reply),
    {next_state, session_established, get(?STATE_KEY)}.

%% == pure ODBC code

%% part of init/1
%% Open an ODBC database connection
odbc_connect(SQLServer) ->
    application:ensure_all_started(odbc),
    odbc:connect(SQLServer, [{scrollable_cursors, off}]).

%% == Native PostgreSQL code

%% part of init/1
%% Open a database connection to PostgreSQL
pgsql_connect(Server, Port, DB, Username, Password) ->
    case pgsql:connect([{host, Server},
                        {database, DB},
                        {user, Username},
                        {password, Password},
                        {port, Port},
                        {as_binary, true}]) of
        {ok, Ref} ->
            pgsql:squery(Ref, [<<"alter database ">>, DB, <<" set ">>,
                               <<"standard_conforming_strings='off';">>]),
            {ok, Ref};
        Err ->
            Err
    end.

%% Convert PostgreSQL query result to Erlang ODBC result formalism
pgsql_to_odbc({ok, PGSQLResult}) ->
    case PGSQLResult of
        [Item] -> pgsql_item_to_odbc(Item);
        Items -> [pgsql_item_to_odbc(Item) || Item <- Items]
    end.

pgsql_item_to_odbc({<<"SELECT", _/binary>>, Rows,
                    Recs}) ->
    {selected, [element(1, Row) || Row <- Rows], Recs};
pgsql_item_to_odbc({<<"FETCH", _/binary>>, Rows,
                    Recs}) ->
    {selected, [element(1, Row) || Row <- Rows], Recs};
pgsql_item_to_odbc(<<"INSERT ", OIDN/binary>>) ->
    [_OID, N] = str:tokens(OIDN, <<" ">>),
    {updated, jlib:binary_to_integer(N)};
pgsql_item_to_odbc(<<"DELETE ", N/binary>>) ->
    {updated, jlib:binary_to_integer(N)};
pgsql_item_to_odbc(<<"UPDATE ", N/binary>>) ->
    {updated, jlib:binary_to_integer(N)};
pgsql_item_to_odbc({error, Error}) -> {error, Error};
pgsql_item_to_odbc(_) -> {updated, undefined}.

%% == Native MySQL code

%% part of init/1
%% Open a database connection to MySQL
mysql_connect(Server, Port, DB, Username, Password) ->
    case p1_mysql_conn:start(binary_to_list(Server), Port,
                             binary_to_list(Username), binary_to_list(Password),
                             binary_to_list(DB), fun log/3)
    of
        {ok, Ref} ->
            p1_mysql_conn:fetch(Ref, [<<"set names 'utf8mb4';">>],
                                self()),
            {ok, Ref};
        Err -> Err
    end.

%% Convert MySQL query result to Erlang ODBC result formalism
mysql_to_odbc({updated, MySQLRes}) ->
    {updated, p1_mysql:get_result_affected_rows(MySQLRes)};
mysql_to_odbc({data, MySQLRes}) ->
    mysql_item_to_odbc(p1_mysql:get_result_field_info(MySQLRes),
                       p1_mysql:get_result_rows(MySQLRes));
mysql_to_odbc({error, MySQLRes})
  when is_binary(MySQLRes) or is_list(MySQLRes) ->
    {error, MySQLRes};
mysql_to_odbc({error, MySQLRes}) ->
    {error, p1_mysql:get_result_reason(MySQLRes)}.

%% When tabular data is returned, convert it to the ODBC formalism
mysql_item_to_odbc(Columns, Recs) ->
    {selected, [element(2, Column) || Column <- Columns], Recs}.

to_odbc({selected, Columns, Recs}) ->
    {selected, Columns, [tuple_to_list(Rec) || Rec <- Recs]};
to_odbc(Res) ->
    Res.

log(Level, Format, Args) ->
    case Level of
        debug -> ?DEBUG(Format, Args);
        normal -> ?INFO_MSG(Format, Args);
        error -> ?ERROR_MSG(Format, Args)
    end.

max_queue() ->
    application:get_env(db_pool, max_queue, 1000).

fsm_limit_opts() ->
    case max_queue() of
        N when is_integer(N) -> [{max_queue, N}];
        _ -> []
    end.
