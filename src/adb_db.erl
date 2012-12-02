-module(adb_db).

-export([start/0, stop/0, snapshot/0, snapshot/1, fail/1, recover/1, rl_acquire/2, wl_acquire/2, release/2, status/1]).

start() ->
	RLockTableId = ets:new(rlock, [set, public]),
	WLockTableId = ets:new(wlock, [set, public]),
	ok = createServer(10, 1, RLockTableId, WLockTableId).

stop() ->
    ok.
 
rl_acquire(TransId, VarId) ->
    rpc({rl_acquire, TransId, VarId}).   
    
wl_acquire(TransId, VarId) ->
    rpc({wl_acquire, TransId, VarId}).
    
%%--------------------------------------------------------------------
%% Function: release(VarId) -> {ok, TransId}
%%--------------------------------------------------------------------
release(TransId, VarId) ->
    rpc({release, TransId, VarId}).   

%%--------------------------------------------------------------------
%% Function: snapshot(SiteId) -> {ok, SiteId, Variables}
%%--------------------------------------------------------------------
snapshot(SiteIdx) ->
    rpc(getId(SiteIdx), {snapshot, SiteIdx}).
    
%%--------------------------------------------------------------------
%% Function: snapshot() -> {ok, {...}}
%%--------------------------------------------------------------------
snapshot() ->
    {ok, {}}.    
	
%%--------------------------------------------------------------------
%% Function: fail() -> {ok, Time}
%%--------------------------------------------------------------------    
fail(SiteIdx) ->
	rpc(getId(SiteIdx), {fail, SiteIdx}).
    
%%--------------------------------------------------------------------
%% Function: recover() -> {ok, Time}
%%--------------------------------------------------------------------    
recover(SiteIdx) ->
   rpc(getId(SiteIdx), {recover, SiteIdx}).
   
status(SiteIdx) ->
   rpc(getId(SiteIdx), {status}).
      
%%====================================================================
%% Internal functions
%%====================================================================

rpc(Sid, Q) ->
	Caller = self(),
    Sid ! {Caller, Q},
    receive
		{Caller, Reply} ->
			Reply
    end.
    
rpc(Q) ->
    adb_db1 ! {self(), Q},
    receive
		{adb_db1, Reply} ->
			Reply
    end.    
    
getId(SiteIdx) ->
	list_to_atom(string:concat("adb_db", integer_to_list(SiteIdx))).
	
createServer(TotalServer, SiteIdx, RLockTableId, WLockTableId) ->
	case SiteIdx =< TotalServer of
		true -> io:format("create site ~p.~n", [getId(SiteIdx)]),
				register(getId(SiteIdx), spawn(fun() -> loop(RLockTableId, WLockTableId, up) end)),
			    createServer(TotalServer, SiteIdx+1, RLockTableId, WLockTableId);
		false -> ok
    end.
    
loop(RLockTableId, WLockTableId, Status) ->
	receive
		{From, {snapshot, SiteIdx}} ->
			io:format("Snapshot: ~p~n", [SiteIdx]),
			From ! {From, ok},
			loop(RLockTableId, WLockTableId, Status);
		{From, {fail, SiteIdx}} ->
			io:format("Fail: ~p from ~p~n", [SiteIdx, From]),
			From ! {From, ok},
			loop(RLockTableId, WLockTableId, down);		
		{From, {recover, SiteIdx}} ->
			io:format("Recover: ~p~n", [SiteIdx]),
			From ! {From, ok},
			loop(RLockTableId, WLockTableId, up);
		{From, {rl_acquire, TransId, VarId}} ->
			case ets:lookup(RLockTableId, VarId) of
				[] -> 
					io:format("Acquired: ~p holds a read lock on ~p~n", [TransId, VarId]),
					ets:insert(RLockTableId, {VarId, TransId}),
					From ! {From, ok};
				[{_, Tid}] ->
					From ! {From, {no, Tid}}
			end,
			loop(RLockTableId, WLockTableId, Status);
		{From, {wl_acquire, TransId, VarId}} ->
			case ets:lookup(WLockTableId, VarId) of
				[] -> 
					io:format("Acquired: ~p holds a write lock on ~p~n", [TransId, VarId]),
					ets:insert(RLockTableId, {VarId, TransId}),
					From ! {From, ok};
				[{_, Tid}] ->
					From ! {From, {no, Tid}}
			end,
			loop(RLockTableId, WLockTableId, Status);			
		{From, {release, VarId}} ->
			io:format("Release: ~p~n", [VarId]),
			From ! {From, ok},
			loop(RLockTableId, WLockTableId, Status);
		{From, {status}} ->
			io:format("Status: ~p~n", [Status]),
			From ! {From, Status},
			loop(RLockTableId, WLockTableId, Status)			
	end.
