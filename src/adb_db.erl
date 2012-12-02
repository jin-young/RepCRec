-module(adb_db).

-export([start/0, stop/0, snapshot/0, snapshot/1, fail/1, recover/1, rl_acquire/2, wl_acquire/2, release/2, status/1]).

start() ->
    spawn(fun() -> createTable() end),
	ok = createServer(10, 1, rlock, wlock).

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

createTable() ->
	ets:new(rlock, [named_table, public, set]),
	ets:new(wlock, [named_table, public, set]),
	receive
	    _ -> []
	end.

rpc(Sid, Q) ->
	Caller = self(),
    Sid ! {Caller, Q},
    receive
		{Caller, Reply} ->
			Reply
    end.
    
rpc(Q) ->
    Caller = self(),
    adb_db1 ! {Caller, Q},
    receive
		{Caller, Reply} ->
			Reply
    end.    
    
getId(SiteIdx) ->
	list_to_atom(string:concat("adb_db", integer_to_list(SiteIdx))).
	
createServer(TotalServer, SiteIdx, RLockTableId, WLockTableId) ->
	case SiteIdx =< TotalServer of
		true -> io:format("create site ~p.~n", [getId(SiteIdx)]),
				register(getId(SiteIdx), spawn(fun() -> loop(up) end)),
			    createServer(TotalServer, SiteIdx+1, RLockTableId, WLockTableId);
		false -> ok
    end.
    
loop(Status) ->
	receive
		{From, {snapshot, SiteIdx}} ->
			io:format("Snapshot: ~p~n", [SiteIdx]),
			From ! {From, ok},
			loop(Status);
		{From, {fail, SiteIdx}} ->
			io:format("Fail: ~p from ~p~n", [SiteIdx, From]),
			From ! {From, ok},
			loop(down);		
		{From, {recover, SiteIdx}} ->
			io:format("Recover: ~p~n", [SiteIdx]),
			From ! {From, ok},
			loop(up);
		{From, {rl_acquire, TransId, VarId}} ->
		    case ets:lookup(wlock, VarId) of
                [] ->
			        case ets:lookup(rlock, VarId) of
				        [] -> 
					        io:format("Acquired: ~p holds a read lock on ~p~n", [TransId, VarId]),
					        ets:insert(rlock, {VarId, [TransId]}),
					        From ! {From, {true, [TransId]}};
				        [{_, Tids}] ->
				            case lists:member(TransId, Tids) of
				                true ->
            				        io:format("Acquired: ~p already holds a read lock on ~p~n", [TransId, VarId]),
	            			        From ! {From, {true, Tids}};
	            			    false ->
	            			        ets:insert(rlock, {VarId, lists:append(Tids, [TransId])}),
	            			        [{_, C}] = ets:lookup(rlock, VarId),
					                From ! {From, {true, C}}
				            end
			        end;
                [{_, Tid}] ->
		            case Tid =:= TransId of
		                true ->
		                    io:format("Acquired: ~p already holds a write lock on ~p~n", [TransId, VarId]),
		                    From ! {From, {true, [TransId]}};
	                    false ->
		                    io:format("Acquiring Failed: ~p already holds a write lock on ~p~n", [Tid, VarId]),
		                    From ! {From, {false, [Tid]}}
		            end
            end,	                        
			loop(Status);
		{From, {wl_acquire, TransId, VarId}} ->
			case ets:lookup(wlock, VarId) of
                [] ->
			        case ets:lookup(rlock, VarId) of
				        [] -> 
					        io:format("Acquired: ~p holds a write lock on ~p~n", [TransId, VarId]),
					        ets:insert(wlock, {VarId, TransId}),
					        From ! {From, {true, [TransId]}};
				        [{_, [H|TL]}] ->
				            case TL of
				                [] ->
				                    case H =:= TransId of
				                        true ->
				                            io:format("Acquired: ~p already holds a read lock on ~p~n", [TransId, VarId]),
				                            ets:insert(wlock, {VarId, TransId}),
				                            From ! {From, {true, [TransId]}};
				                        false ->
				                            io:format("Acquiring Failed: ~p holds a read lock on ~p~n", [H, VarId]),
					                        From ! {From, {false, [H]}}
					                end;
                                _ ->
                                    io:format("Acquiring Failed: there are multiple read locks on ~p~n", [VarId]),
					                From ! {From, {false, [H|TL]}}
					       end
			        end;
                [{_, Tid}] ->
		            case Tid =:= TransId of
		                true ->
		                    io:format("Acquired: ~p already holds a write lock on ~p~n", [TransId, VarId]),
		                    From ! {From, {true, [TransId]}};
	                    false ->
		                    io:format("Acquiring Failed: ~p already holds a write lock on ~p~n", [Tid, VarId]),
		                    From ! {From, {false, [Tid]}}
		            end
            end,	                        
			loop(Status);		
		{From, {release, TransId, VarId}} ->
			io:format("Release: ~p~n", [VarId]),
			case ets:lookup(wlock, VarId) of
			    [] -> [];
                [{_, Tid}] ->
                    io:format("Released write lock on ~p hold by ~p~n", [VarId, Tid]),
                    ets:delete(wlock, VarId)
            end,
            case ets:lookup(rlock, VarId) of
			    [] -> [];
                [{_, Tids}] ->
                    NewTids = lists:delete(TransId, Tids),
                    io:format("Released read lock on ~p hold by ~p~n", [VarId, TransId]),
                    case NewTids of 
                        [] ->
                            ets:delete(rlock, VarId);
                        _ ->
                            ets:insert(rlock, {VarId, NewTids})
                    end
            end,
			From ! {From, true},
			loop(Status);
		{From, {status}} ->
			io:format("Status: ~p~n", [Status]),
			From ! {From, Status},
			loop(Status)			
	end.
	
%% Lock Senario 1
%% adb_db:rl_acquire("T1", x1).  => {true,["T1"]}
%% adb_db:release("T1", x1).  => true
%% adb_db:rl_acquire("T2", x1).  => {true,["T2"]}
%% adb_db:wl_acquire("T2", x1).  => {true,["T2"]}
%% adb_db:wl_acquire("T1", x1).  => {false,["T2"]}
%% adb_db:release("T2", x1).     => true
%% adb_db:wl_acquire("T1", x1).  => {true,["T1"]}
%% adb_db:rl_acquire("T1", x1).  => {true,["T1"]}
%% adb_db:rl_acquire("T1", x1).  => {false,["T1"]}

%% Lock Senario 2
%% adb_db:rl_acquire("T1",x1).
%% adb_db:rl_acquire("T2",x1).
%% adb_db:wl_acquire("T3",x1). => {false,["T1","T2"]}
%% adb_db:wl_acquire("T2",x1). => {false,["T1","T2"]}
%% adb_db:release("T1",x1).
%% adb_db:release("T2",x1).
%% adb_db:wl_acquire("T3",x1). => {true,["T3"]}
