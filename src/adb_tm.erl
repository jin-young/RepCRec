-module(adb_tm).
%-export([start/0, stop/0, beginT/1, endT/1, write/3, read/2, beginRT/1, dump/0, dump/1, fail/1, recover/1).
-export([start/0, beginT/1, endT/1, w/3, r/2, beginRO/1, dump/0, dump/1, fail/1, recover/1]).


start() ->
     register(adb_tm, spawn(fun() ->
				     loop(AgeList=[], WaitList=[], WriteLock=[], ReadLock=[], AccessList=[]) end)).
beginT(Tid) ->
	rpc({beginT, Tid}).
		
beginRO(Tid) ->
	rpc({beginRO, Tid}).

dump() ->
    rpc({dump}).
	
dump(Tid) ->
    rpc({dump,Tid}).
	
fail(Sid) ->
    rpc({fail,Sid}).

recover(Sid) ->
	rpc({recover,Sid}).
	
r(Tid, ValId) ->
	rpc({r,{Tid,ValId}}).

w(Tid, ValId, Value) ->
	rpc({w, {Tid, ValId, Value}}).

endT(Tid) ->
	rpc({endT,Tid}).
	
registerT(Tid) ->
    put(Tid, {ok}).

lookupT(Tid) ->
    get(Tid).

rpc(Q) ->
    adb_tm ! {self(), Q},
    receive
	{adb_tm, Reply} ->
	    Reply
    end.


loop(AgeList, WaitList, WriteLock, ReadLock, AccessList) ->
    receive
	{From, {beginT, TransId}} ->
		From ! {adb_tm, TransId},
	    %if lookupT(TransId) == undefined ->
		    %NewTransId = spawn(fun() -> adb_tran:start()), registerT(newTransId), From ! {ok};
		%io:format("~s~n", [AgeList]),
		loop(lists:append(AgeList,[TransId]), WaitList, WriteLock, ReadLock, AccessList);
	
	{From, {endT, Tid}} ->
		From ! {adb_tm, Tid},
	    loop(AgeList, WaitList, WriteLock, ReadLock, AccessList);
		
	{From, {w, {Tid, ValId, Value}}} ->
		From ! {adb_tm, {Tid, ValId, Value}},
		% write operation
		% Lock = [{t1,x1},{t2,x2},{t3,x5},{t4,x9}].
		% Exist = fun(X) -> (fun({T,Xtmp}) -> Xtmp =:= X end) end.
		% lists:map(Exist(x2), Lock).
	    % lists:member(true, lists:map(Exist(x7), B)).
		WriteLockExist = fun(X) -> (fun({T,Xtmp}) -> Xtmp =:= X end) end,
		%case lists:member(true, lists:map(WriteLockExist(ValId), WriteLock)) of
		%	true ->
		%		%Lock = {},
		%		[{Trans,ValId}] = lists:filter(WriteLockExist(ValId), WriteLock),
		%		io:format("~s~n", [Trans]);
		%	false ->
		%		% lock free, perform
		%		%Lock = {Tid, ValId},
		%		io:format("perform operation~n")
		%end,
		Lock = fun() -> 
			case lists:filter(WriteLockExist(ValId), WriteLock) of 
				[{Trans, ValId}] ->  WriteLock;
				[] -> lists:append(WriteLock,[{Tid,ValId}])
		end,
		loop(AgeList, WaitList, Lock(WriteLock), ReadLock, AccessList);
	
	{From, {r, {Tid, ValId}}} ->
		From ! {adb_tm, {Tid, ValId}},
	    % read operation
		
		loop(AgeList, WaitList, WriteLock, ReadLock, AccessList);
	
	{From, {beginRO, Tid}} ->
		From ! {adb_tm, Tid},
		% create snapshot isolation
	    loop(lists:append(AgeList,[Tid]), WaitList, WriteLock, ReadLock, AccessList);
	
	{From, {dump}} ->
		From ! {adb_tm, dump},
	    loop(AgeList, WaitList, WriteLock, ReadLock, AccessList);
	
	{From, {dump, Sid}} ->
		From ! {adb_tm, Sid},
	    loop(AgeList, WaitList, WriteLock, ReadLock, AccessList);
    
	{From, {fail, Sid}} ->
		% signal fail to site sid
		From ! {adb_tm, Sid},
	    loop(AgeList, WaitList, WriteLock, ReadLock, AccessList);
	
	{From, {recover, Sid}} ->
		From ! {adb_tm, Sid},
		% signal recover to site sid
	    loop(AgeList, WaitList, WriteLock, ReadLock, AccessList)
    end.
