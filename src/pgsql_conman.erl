-module(pgsql_conman).
-behaviour(conman).
-export([init/1, connect/1, disconnect/1, transaction/3]).

init(Args) -> {no_connection, Args}.

connect({connection, _} = State) -> {ok, State};
connect({no_connection, Args}) ->
	try
		{ok, {connection, pgsql_connection:open(Args)}}
	catch
		throw:Err ->
			{{error, Err}, {no_connection, Args}}
	end.

disconnect({connection, Conn}) -> pgsql_connection:close(Conn);
disconnect({no_connection, _}) -> ok.

transaction(Fun, Args, {connection, Conn} = State) ->
	try
		{'begin', []} = pgsql_connection:simple_query(<<"BEGIN">>, Conn),
		Result = apply(Fun, [Conn | Args]),
		{_, []} = pgsql_connection:simple_query(<<"COMMIT">>, Conn),
		{ok, Result, State}
	catch
		badmatch:{error, closed} ->
			{error, disconnected, State};
		Class:Reason ->
			{'rollback', []} = pgsql_connection:simple_query(<<"ROLLBACK">>, Conn),
			{error, {Class, Reason, erlang:get_stacktrace()}, State}
	end;

transaction(_, _, {no_connection, _} = State) ->
	{error, disconnected, State}.
