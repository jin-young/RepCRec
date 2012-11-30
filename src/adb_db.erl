-module(adb_db).

-export([start/0, stop/0, snapshot/0, snapshot/1, fail/1, recover/1, loop/0]).

start() ->
	ok = createServer(10, 1).

stop() ->
    ok.

%%--------------------------------------------------------------------
%% Function: snapshot(SiteId) -> {ok, SiteId, Variables}
%%--------------------------------------------------------------------
snapshot(SiteIdx) ->
    rpc({snapshot, SiteIdx}).
    
%%--------------------------------------------------------------------
%% Function: snapshot() -> {ok, {...}}
%%--------------------------------------------------------------------
snapshot() ->
    {ok, {}}.    
	
%%--------------------------------------------------------------------
%% Function: fail() -> {ok, Time}
%%--------------------------------------------------------------------    
fail(SiteIdx) ->
		getId(SiteIdx) ! {self, {fail, SiteIdx}}.
    
%%--------------------------------------------------------------------
%% Function: recover() -> {ok, Time}
%%--------------------------------------------------------------------    
recover(SiteIdx) ->
   rpc({fail, SiteIdx}).
   
%%====================================================================
%% Internal functions
%%====================================================================

rpc(Q) ->
    adb_db1 ! {self(), Q},
    receive
	{adb_db1, Reply} ->
	    Reply
    end.
    
getId(SiteIdx) ->
	list_to_atom(string:concat("adb_db", integer_to_list(SiteIdx))).
	
createServer(TotalServer, SiteIdx) ->
	case SiteIdx =< TotalServer of
		true -> io:format("create site ~p.~n", [getId(SiteIdx)]),
				register(getId(SiteIdx), spawn(fun() -> loop() end)),
			    createServer(TotalServer, SiteIdx+1);
		false -> ok
    end.
    
loop() ->
	receive
		{From, {snapshot, SiteIdx}} ->
			io:format("Snapshot: ~p~n", [SiteIdx]),
			loop();
		{From, {fail, SiteIdx}} ->
			io:format("Fail: ~p~n", [SiteIdx]),
			loop();		
		{From, {recover, SiteIdx}} ->
			io:format("Recover: ~p~n", [SiteIdx]),
			loop()			
	end.
