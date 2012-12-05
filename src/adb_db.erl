-module(adb_db).

-export([start/0, stop/0, dump/0, dump/1, dumpValue/1, snapshot/0, fail/1, rpc/2, getId/1, 
         recover/1, rl_acquire/2, wl_acquire/2,  release/1, release/2, status/1, getId/1,
         getter/1, setter/2, anySiteFail/0, allSiteFail/0, version/1, getRecentlyUpdatedSite/6]).

start() ->
    spawn(fun() -> createTable() end),
	createServer(10, 1).

stop() ->
    ok.
 
rl_acquire(TransId, VarId) ->
    case getter(VarId) of
        {true, _} -> rpc({rl_acquire, TransId, VarId});
        {false} -> {false}
    end.
     
    
wl_acquire(TransId, VarId) ->
    case getter(VarId) of
        {true, _} -> rpc({wl_acquire, TransId, VarId});
        {false} -> {false}
    end.

%%--------------------------------------------------------------------
%% Function: anySiteFail() -> true | false
%%-------------------------------------------------------------------- 
anySiteFail() ->
    checkAllSiteHealth(1,10).
    
%%--------------------------------------------------------------------
%% Function: allSiteFail() -> true | false
%%-------------------------------------------------------------------- 
allSiteFail() ->
    checkAllSiteFail(1,10).

%%--------------------------------------------------------------------
%% Function: getter(VarId) -> {true, Value} | {false}
%%-------------------------------------------------------------------- 
getter(VarId) ->
    findVariable(1, 10, VarId).

%%--------------------------------------------------------------------
%% Function: setter(VarId, NewValue) -> true | false
%%--------------------------------------------------------------------    
setter(VarId, NewValue) ->
    case getter(VarId) of
        {true, _} -> updateValue(1, 10, VarId, NewValue);
        {false} -> false
    end.
    
updateValue(CurrentIdx, EndIdx, VarId, NewValue) ->
    case CurrentIdx =< EndIdx of
		true -> 
		    case status(CurrentIdx) of
		        up ->
		            rpc(getId(CurrentIdx), {setter, VarId, NewValue}),
		            updateValue(CurrentIdx+1, EndIdx, VarId, NewValue);
		        down ->
		            updateValue(CurrentIdx+1, EndIdx, VarId, NewValue)
		    end;
		false -> true
    end.
    
%%--------------------------------------------------------------------
%% Function: release(TransId, VarId) -> ok
%%--------------------------------------------------------------------
release(TransId, VarId) ->
    rpc({release, TransId, VarId}).   
  
%%--------------------------------------------------------------------
%% Function: release(TransId) -> ok
%%--------------------------------------------------------------------  
release(TransId) ->
    rpc({releaseAll, TransId}).       

%%--------------------------------------------------------------------
%% Function: dump(SiteId) -> {SiteId, up|down, [{VariableId, Value}]}
%%--------------------------------------------------------------------
dump(SiteIdx) ->
    rpc(getId(SiteIdx), {dump}).
    
%%--------------------------------------------------------------------
%% Function: dump() -> [{SiteId, up|down, [{VariableId, Value}]}]
%%--------------------------------------------------------------------
dump() ->
    collectValues(1, 10, []).

%%--------------------------------------------------------------------
%% Function: dumpValue(VariableId) -> [VariableId, [{SiteId, Value}]]
%%--------------------------------------------------------------------   
dumpValue(ValId) ->
    collectVariableFromEachSite(1, 10, ValId, []).
	
%%--------------------------------------------------------------------
%% Function: fail(SiteIdx) -> true
%%--------------------------------------------------------------------    
fail(SiteIdx) ->
	rpc(getId(SiteIdx), {fail}).
    
%%--------------------------------------------------------------------
%% Function: recover(SiteIdx) -> true
%%--------------------------------------------------------------------    
recover(SiteIdx) ->
   rpc(getId(SiteIdx), {recover}).
 
%%--------------------------------------------------------------------
%% Function: status() -> up|down
%%--------------------------------------------------------------------    
status(SiteIdx) ->
   rpc(getId(SiteIdx), {status}).
   
%%--------------------------------------------------------------------
%% Function: snapshot() -> [{VariableId, Value}]
%%-------------------------------------------------------------------- 
snapshot() ->
    ets:new(temptbl, [named_table, set, public]),
    collectSanpshotValues(dump(), temptbl),
    RetVal = ets:tab2list(temptbl),
    ets:delete(temptbl),
    RetVal.
      
%%====================================================================
%% Internal functions
%%====================================================================

initialize(SiteIdx) ->
    rpc(getId(SiteIdx), {initialize}).
    
findVariable(CurrentIdx, EndIdx, VarId) ->
    case CurrentIdx =< EndIdx of
		true -> 
		        Ret = rpc(getId(CurrentIdx), {getter, VarId}),
		        case Ret of
		            {false, _} -> findVariable(CurrentIdx+1, EndIdx, VarId) ;
		            {true, Value} -> {true, Value}
		        end;
		false -> {false}
    end.
    
collectVariableFromEachSite(CurrentIdx, EndIdx, VarId, CollectedValues) ->
    case CurrentIdx =< EndIdx of
		true -> 
		        Ret = rpc(getId(CurrentIdx), {getter, VarId}),
		        case Ret of
		            {false, _} -> collectVariableFromEachSite(CurrentIdx+1, EndIdx, VarId, CollectedValues) ;
		            {true, Value} -> collectVariableFromEachSite(CurrentIdx+1, EndIdx, VarId, lists:append(CollectedValues, [{CurrentIdx, Value}]))
		        end;
		false -> CollectedValues
    end.
    
checkAllSiteHealth(CurrentIdx, EndIdx) ->
    case CurrentIdx =< EndIdx of
		true -> 
		        case status(CurrentIdx) of
		            up -> checkAllSiteHealth(CurrentIdx+1, EndIdx);
		            down -> true
		        end;
		false -> false
    end.
    
checkAllSiteFail(CurrentIdx, EndIdx) ->
    case CurrentIdx =< EndIdx of
		true -> 
		        case status(CurrentIdx) of
		            up -> false;
		            down -> checkAllSiteFail(CurrentIdx+1, EndIdx)
		        end;
		false -> true
    end.
    
collectSanpshotValues(Dump, TempTableId) ->
    case Dump of
        [SiteDump|TailList] ->
             {_, _, Vals} = SiteDump,
            case SiteDump of
                {_, up, Vals} -> 
                    case Vals of
                        [H|TL] ->
                            ets:insert(TempTableId, [H|TL])
                    end;
                {_, down, _} -> []
            end,
            collectSanpshotValues(TailList, TempTableId);
        [] -> TempTableId
    end.
    
collectValues(CurrentIdx, EndIdx, Values) ->
    case CurrentIdx =< EndIdx of
		true -> 
		        collectValues(CurrentIdx+1, EndIdx, lists:append(Values, [dump(CurrentIdx)]));
		        %case status(CurrentIdx) of
		        %    up -> collectValues(CurrentIdx+1, EndIdx, lists:append(Values, [dump(CurrentIdx)]));
		        %    down -> collectValues(CurrentIdx+1, EndIdx, lists:append(Values, [{CurrentIdx, down}]))
		        %end;
		false -> Values
    end.
    
createTable() ->
	ets:new(rlock, [named_table, public, set]),
	ets:new(wlock, [named_table, public, set]),
	ets:new(transHandler, [named_table, public, set]),
	receive
	    _ -> []
	end.

findUpSite(SiteId, BeginId) ->
    case status(SiteId) of
        up ->
            SiteId;
        down ->
            case ((SiteId + 1) rem 10) of
                BeginId ->
                    {false, alldown};
                _ ->
                    findUpSite((SiteId + 1) rem 10, BeginId)
            end
    end.
    
getRecentlyUpdatedSite(CurrentIdx, EndIdx, HeightSite, HeigestVersion, CurrentSiteIdx, CurrentSiteVersion) ->
     case CurrentIdx =< EndIdx of
		true -> 
		    case CurrentIdx =:= CurrentSiteIdx of
		        true ->
		            V = CurrentSiteVersion;
		        false ->
                   {_, V} = version(CurrentIdx)
            end, 
            io:format("RET ~p ~n", [V]),
            case V < HeigestVersion of
                true ->
                    getRecentlyUpdatedSite(CurrentIdx+1, EndIdx, HeightSite, HeigestVersion, CurrentSiteIdx, CurrentSiteVersion);
                false ->
                    getRecentlyUpdatedSite(CurrentIdx+1, EndIdx, CurrentIdx, V, CurrentSiteIdx, CurrentSiteVersion)
            end;
		false -> {HeightSite, HeigestVersion}
    end.
    
version(SiteIdx) ->
    io:format("I'm gonna call ~p to get version~n", [getId(SiteIdx)]),
    io:format("It is atom : ~p~n", [is_atom(getId(SiteIdx))]),

    Ret = rpc(getId(SiteIdx), {version}),
    io:format("It is ~p~n", [Ret]),
    Ret.
    
rpc(Sid, Q) ->
	Caller = self(),
    Sid ! {Caller, Q},
    receive
		{Caller, Reply} ->
			Reply
    end.
        
rpc(Q) ->
    Caller = self(),
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Ran = random:uniform(10), 
    TargetId = findUpSite(Ran, Ran),
    case TargetId of
        {false, alldown} ->
            {false, alldown};
        _ ->
            getId(TargetId) ! {Caller, Q},
            receive
		        {Caller, Reply} ->
			        Reply
            end
    end.    
    
getId(SiteIdx) ->
	list_to_atom(string:concat("adb_db", integer_to_list(SiteIdx))).
	
createServer(TotalServer, SiteIdx) ->
	case SiteIdx =< TotalServer of
		true -> 
		        Sid = getId(SiteIdx),
		        io:format("create site ~p.~n", [Sid]),
				register(Sid, spawn(fun() -> loop(SiteIdx, up, 1) end)),
				initialize(SiteIdx),
				createServer(TotalServer, SiteIdx+1);
		false -> ok
    end.
    
releaseAllLocks(TransId, Locks) ->
    case Locks of
        [] -> ok;
        [[Vid, TransList]|TL] ->
            case lists:member(TransId, TransList) of
                true ->
                    NewTids = lists:delete(TransId, TransList),
                    io:format("Released read lock on ~p hold by ~p~n", [Vid, TransId]),
                    case NewTids of 
                        [] ->
                            ets:delete(rlock, Vid);
                        _ ->
                            ets:insert(rlock, {Vid, NewTids})
                    end,
                    releaseAllLocks(TransId, TL);
                false ->
                    releaseAllLocks(TransId, TL)
            end
    end.
    
loop(SiteIdx, Status, Version) ->
	receive
	    {From, {version}} ->
	        io:format("~p IN VERSION ~p ~p ~n",[SiteIdx, Status, Version]),
	        case Status of
	            up ->
	                io:format("~p UP VERSION ~p ~p ~n",[SiteIdx, Status, Version]),
	                From ! {From, {up, Version}};
	            down ->
	                io:format("~p DOWN VERSION ~p ~p ~n",[SiteIdx, Status, Version]),
	                From ! {From, {down, Version}}
	        end,
	        io:format("~p ALMOST END VERSION ~p ~p ~n",[SiteIdx, Status, Version]),
	        loop(SiteIdx, Status, Version);
	    {From, {setter, VarId, NewValue}} ->
	        TblId = list_to_atom(string:concat("tbl", integer_to_list(SiteIdx))),
            Ret = ets:lookup(TblId, VarId),
            case Ret of
	            [{VarId, _}] -> 
	                ets:insert(TblId, {VarId, NewValue});
	            [] -> []
	        end,
	        From ! {From, true},
	        loop(SiteIdx, Status, Version+1);
	    {From, {getter, VarId}} ->
	        case Status of
			    up ->
			        TblId = list_to_atom(string:concat("tbl", integer_to_list(SiteIdx))),
			        Ret = ets:lookup(TblId, VarId),
			        case Ret of
			            [{_, Value}] -> From ! {From, {true, Value}};
			            [] -> From ! {From, {false, noExist}}
			        end;
			    down ->
			        From ! {From, {false, down}}
			end,
			loop(SiteIdx, Status, Version);
		{From, {dump}} ->
			io:format("dump: ~p~n", [SiteIdx]),
			TblId = list_to_atom(string:concat("tbl", integer_to_list(SiteIdx))),
			case Status of
			    up ->
			        From ! {From, {SiteIdx, up, ets:tab2list(TblId)}};
			    down ->
			        From ! {From, {SiteIdx, down, ets:tab2list(TblId)}}
			end,
			loop(SiteIdx, Status, Version);
		{From, {fail}} ->
		    case Status of
		        down ->
		            io:format("Site ~p is alredy down:~n", [SiteIdx]);
		        up ->
                    io:format("Fail: ~p from ~p~n", [SiteIdx, From])
            end,
			From ! {From, true},
			loop(SiteIdx, down, Version);		
		{From, {recover}} ->
		    io:format("Who am I : ~p , ~p~n", [SiteIdx, Version]),
		    {LatestSiteId, LatestVersion} = getRecentlyUpdatedSite(1, 10, 1, 1, SiteIdx, Version),
		    case Status of
		        down ->
			        io:format("Recover: ~p~n", [SiteIdx]),
			        case LatestSiteId =:= SiteIdx of
			            true -> [];
			            false -> 
			                {_,_,Vals} = dump(LatestSiteId),
			                io:format("Start synchronization ~p with disk ~p~n", [SiteIdx, LatestSiteId]),
			                TblId = list_to_atom(string:concat("tbl", integer_to_list(SiteIdx))),
			                RecentValues = lists:filter(
                                fun(X) -> 
                                    {Vid, _} = X, 
                                    case list_to_integer(hd(string:tokens(Vid,"x"))) rem 2 of
                                        0 -> % even index variable
                                             true;
                                        _ -> % odd index variable. do not copy
                                            false
                                    end
                                end, Vals),
                            ets:insert(TblId, RecentValues)
			        end;
			    up ->
			        io:format("Site ~p is alredy up:~n", [SiteIdx])
			end,
			From ! {From, true},
			loop(SiteIdx, up, LatestVersion);
		{From, {rl_acquire, TransId, VarId}} ->
		    io:format("Handled by ~p~n", [SiteIdx]),
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
			loop(SiteIdx, Status, Version);
		{From, {wl_acquire, TransId, VarId}} ->
		    io:format("Handled by ~p~n", [SiteIdx]),
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
			loop(SiteIdx, Status, Version);		
		{From, {release, TransId, VarId}} ->
		    io:format("Handled by ~p~n", [SiteIdx]),
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
			loop(SiteIdx, Status, Version);
	    {From, {releaseAll, TransId}} ->
	        io:format("Release all locks hold by ~p~n", [TransId]),
	        ets:match_delete(wlock, {'$1', TransId}), 
	        io:format("All write lock hold by ~p have been released.~n", [TransId]),
	        ReadLocks = ets:select(rlock, [{{'$1','$2'},[],['$$']}]),
	        releaseAllLocks(TransId, ReadLocks),
	        From ! {From, true},
	        loop(SiteIdx, Status, Version);
		{From, {status}} ->
			io:format("Status: ~p~n", [Status]),
			From ! {From, Status},
			loop(SiteIdx, Status, Version);
		{From, {initialize}} ->
		    io:format("Initialize site ~p~n", [SiteIdx]),
		    TblId = list_to_atom(string:concat("tbl", integer_to_list(SiteIdx))),
		    ets:new(TblId, [named_table, set]),
		    ets:insert(TblId, {"x2", 20}),
		    ets:insert(TblId, {"x4", 40}),
		    ets:insert(TblId, {"x6", 60}),
		    ets:insert(TblId, {"x8", 80}),
		    ets:insert(TblId, {"x10", 100}),
		    ets:insert(TblId, {"x12", 120}),
		    ets:insert(TblId, {"x14", 140}),
		    ets:insert(TblId, {"x16", 160}),
		    ets:insert(TblId, {"x18", 180}),
		    ets:insert(TblId, {"x20", 200}),
		    case SiteIdx of
		        2 ->
		            ets:insert(TblId, {"x1", 10}),
		            ets:insert(TblId, {"x11", 110});
		        4 ->
		            ets:insert(TblId, {"x3", 30}),
		            ets:insert(TblId, {"x13", 130});		        
		        6 ->
		            ets:insert(TblId, {"x5", 50}),
		            ets:insert(TblId, {"x15", 150});			        
		        8 ->
		            ets:insert(TblId, {"x7", 70}),
		            ets:insert(TblId, {"x17", 170});			        
		        10 ->
		            ets:insert(TblId, {"x9", 90}),
		            ets:insert(TblId, {"x19", 190});
	            _ -> []  
		    end,
		    From ! {From, ok},
		    loop(SiteIdx, Status, Version)
	end.
	
%% Lock Senario 1
%% adb_db:rl_acquire("T1", "x1").  => {true,["T1"]}
%% adb_db:release("T1", "x1").  => true
%% adb_db:rl_acquire("T2", "x1").  => {true,["T2"]}
%% adb_db:wl_acquire("T2", "x1").  => {true,["T2"]}
%% adb_db:wl_acquire("T1", "x1").  => {false,["T2"]}
%% adb_db:release("T2", "x1").     => true
%% adb_db:wl_acquire("T1", "x1").  => {true,["T1"]}
%% adb_db:rl_acquire("T1", "x1").  => {true,["T1"]}
%% adb_db:rl_acquire("T1", "x1").  => {false,["T1"]}

%% Lock Senario 2
%% adb_db:rl_acquire("T1", "x1").
%% adb_db:rl_acquire("T2", "x1").
%% adb_db:wl_acquire("T3", "x1"). => {false,["T1","T2"]}
%% adb_db:wl_acquire("T2", "x1"). => {false,["T1","T2"]}
%% adb_db:release("T1", "x1").
%% adb_db:release("T2", "x1").
%% adb_db:wl_acquire("T3", "x1"). => {true,["T3"]}

%% Release Senario 1
%% adb_db:release("T1", "x1"). => true
%% adb_db:release("T1"). => true

%% snapshot senario 
%% adb_db:snapshot(). => [{"x16",160}, {"x6",60}, {"x20",200}, {"x18",180}....]

%% dump Senario 1
%% adb_db:fail(3).
%% adb_db:fail(4).
%% adb_db:fail(5).
%% adb_db:fail(6).
%% adb_db:dump(). =>
%% [
%%  {1,up, [{"x16",160}, {"x6",60}, {"x20",200}, {"x18",180}, {"x4",40}, {"x14",140}, {"x12",120}, {"x8",80}, {"x2",20}, {"x10",100}]},
%%  {2,up, [{"x11",110}, {"x16",160}, {"x6",60}, {"x20",200}, {"x18",180}, {"x4",40}, {"x1",10}, 
%%          {"x14",140}, {"x12",120}, {"x8",80}, {"x2",20},   {"x10",100}]},
%%  {3,down},
%%  {4,down},
%%  {5,down},
%%  {6,down},
%%  {7,up, [{"x16",160}, {"x6",60}, {"x20",200}, {"x18",180}, {"x4",40}, {"x14",140}, {"x12",120}, {"x8",80}, {"x2",20}, {"x10",100}]},
%%  ....
%% ]

%% dump Senario 2
%% adb_db:dump(3). =>
%% {3,up, [{"x16",160}, {"x6",60}, {"x20",200}, {"x18",180}, {"x4",40}, {"x14",140}, {"x12",120}, {"x8",80}, {"x2",20}, {"x10",100}]}

