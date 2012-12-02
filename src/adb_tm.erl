-module(adb_tm).
%-export([start/0, stop/0, beginT/1, endT/1, write/3, read/2, beginRT/1, dump/0, dump/1, fail/1, recover/1).
-export([start/0, beginT/1, endT/1, w/3, r/2, beginRO/1, dump/0, dump/1, fail/1, recover/1]).


start() ->
     register(adb_tm, spawn(fun() ->
				     loop(AgeList=[], ROList=[], WaitList=[], AccessList=[], AbortList=[]) end)).
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
	
whoOlder(T1, T2, List) ->
	[Head | Tail] = List,
	if 
		Head =:= T1 ->
			T1;
		Head =:= T2 ->
			T2;
		true ->
			whoOlder(T1, T2, Tail)
	end.

% abort function
% clean up function
isMember(Tid, List) ->
	case List of
			[] ->
				false;
			[Head|Tail] -> 
				[Operation|Detail]= Head,
				case Detail of 
					[{Tid,_,_}] -> 
						true;
					[{Tid,_}] ->
						true;
					[{Tid}] ->
						true;
					_ ->
						isMember(Tid, Tail)
				end
	end.				


loop(AgeList, ROList, WaitList, AccessList, AbortList) ->
    receive
	{From, {beginT, TransId}} ->
		From ! {adb_tm, TransId},
	    %if lookupT(TransId) == undefined ->
		    %NewTransId = spawn(fun() -> adb_tran:start()), registerT(newTransId), From ! {ok};
		%io:format("~s~n", [AgeList]),
		loop(lists:append(AgeList,[TransId]),ROList, WaitList, AccessList, AbortList);
	
	{From, {endT, Tid}} ->
		From ! {adb_tm, Tid},
			case lists:member(Tid,AbortList) of
				true -> 
					loop(AgeList, ROList, WaitList, AccessList, lists:delete(Tid, AbortList));
				false ->
					case isMember(Tid, WaitList) of
						true->
							loop(AgeList, ROList, lists:append(WaitList,[{endT, Tid}]), AccessList, AbortList); 
						false->
							%commit and cleanup;
							loop(AgeList, ROList, WaitList, AccessList, AbortList)
					end
			end;
		%loop(AgeList,ROList, WaitList, AccessList, AbortList);
	{From, {w, {Tid, ValId, Value}}} -> 
		From ! {adb_tm, {Tid, ValId, Value}},
		%WriteLockExist = fun(X, T) -> (fun({T,Xtmp}) -> Xtmp =:= X end) end,
		%case lists:member(true, lists:map(WriteLockExist(ValId), WriteLock)) of
		case db:wl_acquire() of  
			{false, THoldLock} ->
				io:format("~s put in to WaitList or abort~n", [Tid]),
				% compare age to WaitList or holding lock transaction
				%[{THoldLock, ValId}] = lists:filter(WriteLockExist(ValId), WriteLock),
				 
				 case whoOlder(THoldLock, Tid, AgeList) =:= Tid of
					true ->
						 % keep in the waitlist
						 loop(AgeList, ROList, lists:append(WaitList,[{Tid, ValId, Value}]), AccessList, AbortList);
					false ->
						% abort the transaction
						loop(AgeList, ROList, WaitList, AccessList, lists:append(AbortList, [Tid]))
				 end;
			true ->
				io:format("~s performed~n", [Tid]),
				loop(AgeList,ROList, WaitList, list:append(AccessList, [w, {Tid, ValId, Value}]), AbortList)	
				% perform operation
		end;
		%loop(AgeList, WaitList, lists:append(WriteLock, addWriteLock(Tid, ValId, WriteLock)), ReadLock, AccessList);
	
	{From, {r, {Tid, ValId}}} ->
		From ! {adb_tm, {Tid, ValId}},
	    % read operation
		case db:rl_acquire() of  
			{false, THoldLock} ->
				io:format("~s put in to WaitList or abort~n", [Tid]),
				% compare age to WaitList or holding lock transaction
				%[{THoldLock, ValId}] = lists:filter(WriteLockExist(ValId), WriteLock),
				 
				 case whoOlder(THoldLock, Tid, AgeList) =:= Tid of
					true ->
						 % keep in the waitlist
						 loop(AgeList,ROList, lists:append(WaitList,[{Tid, ValId}]),  AccessList, AbortList);
					false ->
						% abort the transaction
						loop(AgeList,ROList, WaitList,  AccessList, lists:append(AbortList, [Tid]))
				end;
			true ->
				io:format("~s performed~n", [Tid]),
				loop(AgeList,ROList, WaitList,lists:append(AccessList,[r, {Tid}]), AbortList)	
				% perform operation
		end;
		
		%loop(AgeList, WaitList, WriteLock, ReadLock, AccessList, AbortList);
	
	{From, {beginRO, Tid}} ->
		From ! {adb_tm, Tid},
		% create snapshot isolation
	    loop(lists:append(AgeList,[Tid]),ROList, WaitList, AccessList, AbortList);
	
	{From, {dump}} ->
		From ! {adb_tm, dump},
	    loop(AgeList,ROList, WaitList, AccessList, AbortList);
	
	{From, {dump, Sid}} ->
		From ! {adb_tm, Sid},
	    loop(AgeList,ROList, WaitList, AccessList, AbortList);
    
	{From, {fail, Sid}} ->
		% signal fail to site sid
		From ! {adb_tm, Sid},
	    loop(AgeList,ROList, WaitList, AccessList, AbortList);
	
	{From, {recover, Sid}} ->
		From ! {adb_tm, Sid},
		% signal recover to site sid
	    loop(AgeList,ROList, WaitList, AccessList, AbortList)
    end.
