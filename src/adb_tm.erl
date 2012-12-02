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

checkReadWaitListOlder(Tid, ValId, WaitList, AgeList) ->
	case WaitList of
			[] ->
				true;
			[Head|Tail] -> 
				[Operation|Detail]= Head,
				case Detail of 
					[{_,ValId,_}] ->
						[{THold, ValId,_}] = Detail,
						case  whoOlder(Tid, THold, AgeList) =:= Tid of
							true ->
								checkReadWaitListOlder(Tid, ValId, Tail, AgeList);
							false ->
								false	
						end;
					 _ ->
						checkReadWaitListOlder(Tid, ValId, Tail, AgeList)	
				end
	end.	
	
checkWriteWaitListOlder(Tid, ValId, WaitList, AgeList) ->
	case WaitList of
			[] ->
				true;
			[Head|Tail] -> 
				[Operation|Detail]= Head,
				case Detail of 
					[{_,ValId,_}] ->
						[{THold, ValId,_}] = Detail,
						case  whoOlder(Tid, THold, AgeList) =:= Tid of
							true ->
								checkWriteWaitListOlder(Tid, ValId, Tail, AgeList);
							false ->
								false	
						end;
					[{_,ValId}] ->
						[{THold, ValId}] = Detail,
						case  whoOlder(Tid, THold, AgeList) =:= Tid of
							true ->
								checkWriteWaitListOlder(Tid, ValId, Tail, AgeList);
							false ->
								false	
						end;
					[_] ->
						checkWriteWaitListOlder(Tid, ValId, Tail, AgeList)	
				end
	end.			
	

%cleanUp(Tid, AgeList, ROList, WaitList, AccessList, AbortList) ->
	% release lock -> Tid and ValId that Tid hold from AccessList
	% remove Tid from every list
	% add Tid to abort list
	% new operation from WaitList ask JY
	% remove Tid and its snapshot from ROList
%	loop(lists:delete(Tid,AgeList), ROList, WaitList, AccessList, lists:append(AbortList, [Tid]))
	

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
					[Tid] ->
						true;
					_ ->
						isMember(Tid, Tail)
				end
	end.				

deleteElement(Tid, List, TmpList) ->
	case List of
			[] ->
				TmpList;
			[Head|Tail] -> 
				[Operation|Detail]= Head,
				case Detail of 
					[{Tid,_,_}] -> 
						deleteElement(Tid, lists:delete(Head, List), TmpList);
					[{Tid,_}] ->
						deleteElement(Tid, lists:delete(Head, List), TmpList);
					[Tid] ->
						deleteElement(Tid, lists:delete(Head, List), TmpList);
					_ ->
						deleteElement(Tid, Tail, lists:append(TmpList,[Head]))
				end
	end.				

checkWaitList(AccessList, WaitList, NewWaitList) ->
	case WaitList of
		[] ->
			lists:append([AccessList],[NewWaitList]);
		[Head|Tail] ->
			case Head of
				[w,_] ->
					[w,{Tid, ValId , Value }] = Head,
					case db:wl_acquire(Tid, ValId) of  
							{false, THoldLock} -> 
								checkWaitList(AccessList, Tail, lists:append(NewWaitList, [Head]));
							true ->
								io:format("~s performed~n", [Tid]),
								% perform operation
								checkWaitList(lists:append([Head],AccessList), Tail, deleteElement(Tid,WaitList, []))
					end;
				[r,_] ->
					[r,{Tid, ValId}] = Head,
					% check readonly
					case db:rl_acquire(Tid, ValId) of  
						{false, THoldLock} -> 
							checkWaitList(AccessList, Tail, lists:append(NewWaitList, [Head]));
						true ->
							io:format("~s performed~n", [Tid]),
							% perform operation
							checkWaitList(lists:append([Head],AccessList), Tail, deleteElement(Tid,WaitList, []))
					end;
				[endT, _] -> 		
					[endT,Tid] = Head,
					TmpList = lists:delete([endT,Tid], NewWaitList),
					case isMember(Tid, TmpList) of
						true->
							checkWaitList(AccessList, Tail, NewWaitList);
						false->
							checkWaitList(AccessList, Tail, deleteElement(Tid,WaitList, []))
							%commit and cleanup;						
					end		
				end
	end.


abort(Tid, AgeList, ROList, WaitList, AccessList, AbortList) ->
	% release lock -> Tid and ValId that Tid hold from AccessList
	% remove Tid from every list
	% add Tid to abort list
	% new operation from WaitList ask JYdd
	% release(Tid, ValId),
	
	
	NewWaitList = deleteElement(Tid,WaitList,[]),
	NewAccessList = deleteElement(Tid, AccessList,[]),
	[NewAccessList2|NewWaitList2] = checkWaitList(NewAccessList, NewWaitList, []),
	loop(lists:delete(Tid,AgeList), ROList, NewWaitList2, NewAccessList2, lists:append(AbortList, [Tid])).
	


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
							loop(AgeList, ROList, lists:append(WaitList,[[{endT, Tid}]]), AccessList, AbortList); 
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
				 
				 case whoOlder(THoldLock, Tid, AgeList) =:= Tid andalso checkWriteWaitListOlder(Tid, ValId, WaitList, AgeList) of
					true ->
						 % keep in the waitlist
	 					% we have to check the conflict in the waitlist as well
						 
						 loop(AgeList, ROList, lists:append(WaitList,[[w, {Tid, ValId, Value}]]), AccessList, AbortList);
					false ->
						% abort the transaction
						abort(Tid,AgeList, ROList, WaitList, AccessList, AbortList)
				 end;
			true ->
				io:format("~s performed~n", [Tid]),
				loop(AgeList,ROList, WaitList, list:append([[w, {Tid, ValId, Value}]], AccessList ), AbortList)	
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
				 
				 case whoOlder(THoldLock, Tid, AgeList) =:= Tid andalso checkReadWaitListOlder(Tid, ValId, WaitList, AgeList) of
					true ->
						 % keep in the waitlist
						% we have to check the conflict in the waitlist as well	
						 loop(AgeList,ROList, lists:append(WaitList,[[r, {Tid, ValId}]]),  AccessList, AbortList);
					false ->
						% abort the transaction
						loop(AgeList,ROList, WaitList,  AccessList, lists:append(AbortList, [Tid]))
				end;
			true ->
				io:format("~s performed~n", [Tid]),
				loop(AgeList,ROList, WaitList,lists:append([[r, {Tid}]],AccessList), AbortList)	
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
