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
	
whoOlder(Ttmp1, Ttmp2, List) ->
%	io:format("~p ~p ~p~n",[Ttmp1,Ttmp2, List]),
	[Head | Tail] = List,
	if 
		Head =:= Ttmp1 ->
			Ttmp1;		
		Head =:= Ttmp2 ->	
			Ttmp2;
		true ->
			whoOlder(Ttmp1, Ttmp2, Tail)
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
	
	% If some transactions in WaitList can performed, do it!
checkWaitList(AgeList,ROList,AccessList, WaitList, AbortList, NewWaitList) ->
		%io:format("~p~n", [WaitList]),
		case WaitList of
			[] ->
				lists:append([AccessList],[NewWaitList]);
			[Head|Tail] ->
				%io:format("999999999999999999999~p~n", [Head]),
				case Head of
					[w,_] ->
						%io:format("~p", [Head]),
						[w,{Tid, ValId , Value }] = Head,
						case rpc:call(db@localhost, adb_db, wl_acquire,[Tid,ValId])  of  
								{false, [THoldLock]} -> 
								    io:format("cannot obtain lock on ~s ~n", [ValId]),
									checkWaitList(AgeList, ROList,AccessList, Tail,AbortList, lists:append(NewWaitList, [Head]));
								{false} ->
										io:format("put ~s into WaitList~n", [Tid]),
										checkWaitList(AgeList, ROList,AccessList, Tail,AbortList, lists:append(NewWaitList, [Head]));
										%abort(Tid,AgeList, ROList, lists:append(WaitList,[[w, {Tid, ValId, Value}]]), AccessList, AbortList);
										%loop(AgeList, ROList, lists:append(WaitList,[[w, {Tid, ValId, Value}]]), AccessList, AbortList);
								{true, _} ->
									io:format("~p performed write on ~p~n", [Tid, ValId]),
									% perform operation
									checkWaitList(AgeList, ROList,lists:append([Head],AccessList), Tail, AbortList,NewWaitList)
						end;
					[r,_] ->
						%io:format("~p~n", [Head]),
						[r,{Tid, ValId}] = Head,
						%io:format("~p : ~p~n", [Tid, ValId]),
						%doReadOnly(ROList, WaitList, NewWaitList),
						Ret = rpc:call(db@localhost, adb_db, rl_acquire,[Tid,ValId]),
						%io:format("88888~p ~n", [Ret]),
						case Ret of  
							
							{false, [THoldLock]} -> 
								checkWaitList(AgeList,ROList,AccessList, Tail, AbortList, lists:append(NewWaitList, [Head]));
							{false} ->
									% site fail
									%abort(Tid,AgeList, ROList, lists:append(WaitList,[[r, {Tid, ValId}]]), AccessList, AbortList);
									io:format("put ~s into WaitList~n", [Tid]),
									checkWaitList(AgeList, ROList,AccessList, Tail, AbortList, lists:append(NewWaitList, [Head]));
							{true, _} ->
									io:format("~p performed read on ~p~n", [Tid, ValId]),
									% perform operation
									checkWaitList(AgeList, ROList,lists:append([Head],AccessList), Tail, AbortList,NewWaitList)							
								
						end;
					[endT, _] -> 		
						[endT,Tid] = Head,
						%io:format("00000000000000~p~n", [Tid]),
						%io:format("new Waitlist: ~p ~n", [NewWaitList]),
						%io:format("`Waitlist: ~p ~n", [WaitList]),

						%TmpList = lists:delete([endT,Tid], NewWaitList),
						case isMember(Tid, NewWaitList) of
							true->
								io:format("~s in Waitlist~n", [Tid]),
								
								checkWaitList(AgeList, ROList,AccessList, Tail, AbortList, lists:append(NewWaitList, [Head]));
							false->
								%commit and cleanup;
								io:format("~s commited~n", [Tid]),
								% commit 
								ReverseAccessList = lists:reverse(AccessList),
								% clean up
								cleanUp(Tid, AgeList, ROList, WaitList,lists:reverse(commit(Tid, ReverseAccessList, [])), AbortList),	
								checkWaitList(AgeList, ROList,AccessList, Tail,AbortList, NewWaitList)					
						end		
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

isReadOnly(Tid, ROList) ->
	case ROList of 
		[] ->
			false;
		[H | TL] ->
			[Ttmp | Snapshot] = H,
			if 
				Ttmp =:= Tid ->	 
					true;
				true ->
					isReadOnly(Tid, TL)
			end
	end.
	
	evenNum(ValId) ->
		%io:format("~p~n", [ValId]),
		Index = hd(string:tokens(ValId,"x")),
		%io:format("~p~n", [Index]),
		Id = list_to_integer(Index),
		%io:format("~p~n", [Id]),
		if 
			Id rem 2 =:= 0 ->
				% even
				%io:format("true ~p~n", [Id]),
				{true};
			true ->
				% odd
				%io:format("flase ~p~n", [Id]),
				if 
					Id =:= 9 -> {false,integer_to_list(10)};
					Id =:= 19 -> {false,integer_to_list(10)};
					true -> {false,integer_to_list((Id + 1) rem 10)} 
				end
		end.	

readFromSnapshot(Tid, ValId, ROList) ->
 
			[H | TL] = ROList,
			[Ttmp | Snapshot] = H,
			if 
				Ttmp =:= Tid ->	 
					Pred = fun(X) -> (fun({Xtmp,_}) -> Xtmp =:= X end) end,
					case lists:filter(Pred(ValId), Snapshot) of
						[] -> 
							case rpc:call(db@localhost, adb_db, getter,[ValId]) of 
								{true,Value} -> {true,Value}; 
								{false} -> {false}
							end;
						[{ValId,Value}] -> {true,Value}
					end;   
				true ->
					readFromSnapshot(Tid, ValId, TL)
			end.
	
	
			
doReadOnly(ROList, List, NewList) ->
	case List of
			[] ->
				NewList;
			[Head|Tail] -> 
				[Operation|Detail]= Head,
				case Detail of 
					[{Ttmp,ValId}] ->
						case isReadOnly(Ttmp, ROList) of
							true ->
								% Perform Read-only + check site not fail
								case evenNum(ValId) of
									{true} ->
										case rpc:call(db@localhost, adb_db, anySiteFail,[]) of 
											true -> 
												doReadOnly(ROList, Tail, lists:append(NewList, [Head]));
											false -> 
												case readFromSnapshot(Ttmp, ValId, ROList) of
													{true,Value} -> 
														doReadOnly(ROList, Tail, NewList);
														% cal rpc to client to send value
													{false} -> 
														doReadOnly(ROList, Tail, lists:append(NewList, [Head]))
												end	
										end;
									{false, Sid} ->	
										case rpc:call(db@localhost, adb_db, status,[list_to_integer(Sid)]) of
											up -> 
												doReadOnly(ROList, Tail, lists:append(NewList, [Head]));
											down ->
												case readFromSnapshot(Ttmp, ValId, ROList) of
													{true,Value} -> 
														doReadOnly(ROList, Tail, NewList),
														% cal rpc to client to send value
														io:format("call rpc to client ~p : ~p~n", [ValId, Value]);
													{false} -> 
														doReadOnly(ROList, Tail, lists:append(NewList, [Head]))
												end	
										end
								end;
							false ->
								doReadOnly(ROList, Tail, lists:append(NewList, [Head]))	
						end;
					 _ ->
						doReadOnly(ROList, Tail, lists:append(NewList, [Head]))		
				end
	end.	
		

isMember(Tid, List) ->
	%io:format("~s , ~p", [Tid, List]),
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

	cleanUpRO(Tid, ROList, TmpROList) ->
		case ROList of 
			[] ->
				TmpROList;
			[H | TL] ->
				[Ttmp | Snapshot] = H,
				if 
					Ttmp =:= Tid ->	 
						cleanUpRO(Tid, TL, TmpROList);
					true ->
						cleanUpRO(Tid, TL, lists:append(TmpROList,[H]) )
				end
		end.

commit(Tid, List, NewAccessList) ->
	case List of
		[] ->
			NewAccessList;
		
		[H | TL] ->
			[Operation | [Detail]] = H,
			if 
			Operation =:= r ->
				commit(Tid, TL, NewAccessList);
			true ->
				{Ttmp,ValId,Value} = Detail,
				if 
					Ttmp =:= Tid ->	 
						rpc:call(db@localhost, adb_db, setter,[ValId, Value]),
						commit(Tid, TL, NewAccessList);
					true ->
						commit(Tid, TL, lists:append(NewAccessList,[H]) )
				end
			end
	end.

readTrack(Tid, ValId, AccessList) ->		

	case AccessList of
		[] ->
			case rpc:call(db@localhost, adb_db, getter,[ValId]) of 
				{true,Value} -> 
					% cal rpc to client to send value
					io:format("call rpc to client ~p : ~p~n", [ValId, Value]);
				{false} -> 
					% put it in waitlist
					io:format("put into ~p : ~n", [ValId])
					
			end;
		[Head|Tail] ->
			case Head of
				[w,_] ->
					%io:format("~p", [Head]),
					[w,{Ttmp, ValIdtmp , Value }] = Head,
					case Tid =:= Ttmp andalso ValId =:= ValIdtmp of
						true ->
							% cal rpc to client to send value
							io:format("call rpc to client ~p : ~p~n", [ValId, Value]);
						false ->
							readTrack(Tid, ValId, Tail)
					end;	
				_ ->
					readTrack(Tid, ValId, Tail)
				end
	end.
	
		
		

abort(Tid, AgeList, ROList, WaitList, AccessList, AbortList) ->
	% release lock -> Tid and ValId that Tid hold from AccessList
	% remove Tid from every list
	% add Tid to abort list
	% new operation from WaitList ask JYdd
	% release(Tid, ValId),
	% rpc:call(db@localhost, adb_db, wl_acquire,[Tid,ValId])
	% rpc:call(db@localhost, adb_db, release,Tid),
	io:format("~s aborted in abort function~n", [Tid]),
	
	% io:format("previous ~p , ~p ~n", [WaitList, AccessList]),
	
	NewWaitList = deleteElement(Tid,WaitList,[]),
	NewAccessList = deleteElement(Tid, AccessList,[]),	

	% io:format("new ~p , ~p ~n", [NewWaitList, NewAccessList]),
	rpc:call(db@localhost, adb_db, release,[Tid]),
	[ NewAccessList2| [ NewWaitList2 ] ] = checkWaitList(AgeList, ROList,NewAccessList, NewWaitList,AbortList, []),
	% io:format("new ~p , ~p ~n", [NewWaitList2, NewAccessList2]),
	loop(lists:delete(Tid,AgeList), ROList, NewWaitList2, NewAccessList2, lists:append(AbortList, [Tid])).
	
cleanUp(Tid, AgeList, ROList, WaitList, AccessList, AbortList) ->
		% release lock -> Tid and ValId that Tid hold from AccessList
		% remove Tid from every list
		% new operation from WaitList ask JY
		% remove Tid and its snapshot from ROList
		%	loop(lists:delete(Tid,AgeList), ROList, WaitList, AccessList, lists:append(AbortList, [Tid]))
		io:format("~s cleaned up~n", [Tid]),
	
		NewWaitList = deleteElement(Tid,WaitList,[]),
		NewAccessList = deleteElement(Tid, AccessList,[]),	
		% io:format("new ~p , ~p ~n", [NewWaitList, NewAccessList]),
		rpc:call(db@localhost, adb_db, release,[Tid]),
		[ NewAccessList2| [ NewWaitList2 ] ] = checkWaitList(AgeList, ROList,NewAccessList, NewWaitList,AbortList, []),
		% io:format("new ~p , ~p ~n", [NewWaitList2, NewAccessList2]),
		loop(lists:delete(Tid,AgeList), cleanUpRO(Tid, ROList, []), NewWaitList2, NewAccessList2, lists:delete(Tid, AbortList)).
		
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
		%io:format("~s", [Tid]),
			case lists:member(Tid,AbortList) of
				true -> 
					loop(AgeList, ROList, WaitList, AccessList, lists:delete(Tid, AbortList));
				false ->
					%io:format("not in abortlist--------~p~n", [Tid]),
					case isMember(Tid, WaitList) of
						true->
							io:format("put in the waitlist~n"),
							loop(AgeList, ROList, lists:append(WaitList,[[endT, Tid]]), AccessList, AbortList); 
						false->
							io:format("~s commited~n", [Tid]),
							% commit 
							ReverseAccessList = lists:reverse(AccessList),
							% clean up
							cleanUp(Tid, AgeList, ROList, WaitList,lists:reverse(commit(Tid, ReverseAccessList, [])), AbortList)
							%loop(AgeList, ROList, WaitList, AccessList, AbortList)
					end
			end;
		%loop(AgeList,ROList, WaitList, AccessList, AbortList);
		
		
	{From, {w, {Tid, ValId, Value}}} -> 
		From ! {adb_tm, {Tid, ValId, Value}},
		%WriteLockExist = fun(X, T) -> (fun({T,Xtmp}) -> Xtmp =:= X end) end,
		%case lists:member(true, lists:map(WriteLockExist(ValId), WriteLock)) of
		%io:format("~p~n ", [AccessList]),
		case lists:member(Tid,AbortList) of
			true -> 
				io:format("~p already aborted~n", [Tid]),
				loop(AgeList, ROList, WaitList, AccessList, AbortList);
			false ->
				io:format("~p ~p ~n", [Tid, ValId]),
				case rpc:call(db@localhost, adb_db, wl_acquire,[Tid,ValId]) of  
					{false, [THoldLock]} ->
					
					% compare age to WaitList or holding lock transaction
					%[{THoldLock, ValId}] = lists:filter(WriteLockExist(ValId), WriteLock),
						case whoOlder(THoldLock, Tid, AgeList) =:= Tid andalso checkWriteWaitListOlder(Tid, ValId, WaitList, AgeList) of
							true ->
						 	   	% keep in the waitlist
	 					 	  	io:format("put ~s into WaitList~n", [Tid]),				 
						 		loop(AgeList, ROList, lists:append(WaitList,[[w, {Tid, ValId, Value}]]), AccessList, AbortList);
							false ->
								% abort the transaction
								abort(Tid,AgeList, ROList, WaitList, AccessList, AbortList)
				 		end;
					{false} ->
						io:format("put ~s into WaitList~n", [Tid]),
						%abort(Tid,AgeList, ROList, lists:append(WaitList,[[w, {Tid, ValId, Value}]]), AccessList, AbortList);
						loop(AgeList, ROList, lists:append(WaitList,[[w, {Tid, ValId, Value}]]), AccessList, AbortList);
					{true,_} ->
						io:format("~p performed write on ~p~n", [Tid, ValId]),
						loop(AgeList,ROList, WaitList, lists:append([[w, {Tid, ValId, Value}]], AccessList ), AbortList)	
						% perform operation
				end
		end;
		%loop(AgeList, WaitList, lists:append(WriteLock, addWriteLock(Tid, ValId, WriteLock)), ReadLock, AccessList);
	
	{From, {r, {Tid, ValId}}} ->
		From ! {adb_tm, {Tid, ValId}},
	    % read operation
		%io:format("~p~n ", [AccessList]),
		
		case lists:member(Tid,AbortList) of
			true -> 
				io:format("~p already aborted~n", [Tid]),
				loop(AgeList, ROList, WaitList, AccessList, AbortList);
			false ->
				%io:format("~p~n", [rpc:call(db@localhost, adb_db, rl_acquire,[Tid,ValId])]),

				% 1: read is from ReadOnly Transaction
				% if site is fail -> in the waitList
				% if recover call checkWaitList
				% 2: read is from normal transaction, tracking from AccessList from that transaction
				%io:format("~p ~p ~p~n",[Tid, ROList,isReadOnly(Tid, ROList)]),
				case isReadOnly(Tid, ROList) of
					true ->
						case readFromSnapshot(Tid, ValId, ROList) of
							{true,Value} -> 
							% cal rpc to client to send value
							io:format("call rpc to client ~p : ~p~n", [ValId, Value]);
							{false} -> 
								loop(AgeList,ROList, lists:append(WaitList,[[r, {Tid, ValId}]]),  AccessList, AbortList)
						end;
					false ->
						case rpc:call(db@localhost, adb_db, rl_acquire,[Tid,ValId])  of  
							{false, [THoldLock]} ->
							% compare age to WaitList or holding lock transaction
							%[{THoldLock, ValId}] = lists:filter(WriteLockExist(ValId), WriteLock),
						
						 		case whoOlder(THoldLock, Tid, AgeList) =:= Tid andalso checkReadWaitListOlder(Tid, ValId, WaitList, AgeList) of
									true ->
								 	   	% keep in the waitlist
									   	% we have to check the conflict in the waitlist as well	
									   	io:format("put ~s into WaitList~n", [Tid]),
								 	  	loop(AgeList,ROList, lists:append(WaitList,[[r, {Tid, ValId}]]),  AccessList, AbortList);
									false ->
										% abort the transaction
										abort(Tid,AgeList, ROList, WaitList, AccessList, AbortList)
									end;
							{false} ->
								% site fail
								%abort(Tid,AgeList, ROList, lists:append(WaitList,[[r, {Tid, ValId}]]), AccessList, AbortList);
								io:format("put ~s into WaitList~n", [Tid]),
								loop(AgeList, ROList, lists:append(WaitList,[[r, {Tid, ValId}]]), AccessList, AbortList);
							{true,_} ->
								io:format("~p performed read on ~p~n", [Tid, ValId]),
								% readTrack!!!
								loop(AgeList,ROList, WaitList,lists:append([[r, {Tid, ValId}]],AccessList), AbortList)	
								% perform operation
						end
				end	
		end;
		%loop(AgeList, WaitList, WriteLock, ReadLock, AccessList, AbortList);
	
	{From, {beginRO, Tid}} ->
		From ! {adb_tm, Tid},
		% create snapshot isolation
	    loop(lists:append(AgeList,[Tid]),lists:append(ROList,[[Tid,rpc:call(db@localhost, adb_db, snapshot, [])]]), WaitList, AccessList, AbortList);
	{From, {dump}} ->
		From ! {adb_tm, dump},
		rpc:call(db@localhost, adb_db, dump, []),
		% return above to client
	    loop(AgeList,ROList, WaitList, AccessList, AbortList);
	
	{From, {dump, Sid}} ->
		From ! {adb_tm, Sid},
		%rpc:call(db@localhost, adb_db, dump, [Sid]),
		% return above to client
                case re:run(Sid, "x.+") of
                    {match,_} ->
                        [A]=string:tokens(Sid,"x"),
                        rpc:call(db@localhost, adb_db, dump,[list_to_integer(A)]);
					nomatch ->
						rpc:call(db@localhost, adb_db, dumpValue, [list_to_integer(Sid)])	
				end,
	    loop(AgeList,ROList, WaitList, AccessList, AbortList);
    
	{From, {fail, Sid}} ->
		From ! {adb_tm, Sid},
		% signal fail to site sid
		% track the AccessList to see what variables are located at site that failed
		rpc:call(db@localhost, adb_db, fail, [list_to_integer(Sid)]),
		%case rpc:call(db@localhost, adb_db, fail, [Sid]) of
		%	true ->
		%		io:format("site true");
		%	false ->
		%		io:format("site flase")
		%end, 
		io:format("site ~p fail~n", [Sid]),
		failTrack(Sid,AgeList,ROList, WaitList, AccessList, AbortList, []);
	    %loop(AgeList,ROList, WaitList, AccessList, AbortList);
	
	{From, {recover, Sid}} ->
		From ! {adb_tm, Sid},
		true = rpc:call(db@localhost, adb_db, recover,[list_to_integer(Sid)]),
		io:format("site: ~p recovered~n", [Sid]),
		[ NewAccessList2 | [ NewWaitList2 ] ] = checkWaitList(AgeList, ROList,AccessList, WaitList,AbortList, []),
	    loop(AgeList,ROList, NewWaitList2, NewAccessList2, AbortList)
    end.

	failTrack(Sid,AgeList, ROList, WaitList, AccessList, AbortList, TmpAccessList) ->
		
		case AccessList of
				[] ->
					loop(AgeList,ROList, WaitList, TmpAccessList, AbortList);
				[Head|Tail] -> 
					[Operation|Detail]= Head,
					%io:format("---------~p~n", [Head]),
					%io:format("---------~p~n", [Detail]),
					case Detail of 
						[{_,ValId,_}] ->
							[{Tid, ValId,_}] = Detail,
							case evenNum(ValId) of
								{true} -> 
									io:format("~s aborted~n", [Tid]),
									NewWaitList = deleteElement(Tid,WaitList,[]),
									NewAccessList = deleteElement(Tid, AccessList,[]),
									rpc:call(db@localhost, adb_db, release,[Tid]),
									[ NewAccessList2| [ NewWaitList2 ] ] = checkWaitList(AgeList, ROList,NewAccessList, NewWaitList,AbortList, []),
									%io:format("W TRUE~n"),
									%io:format("~p ~p~n", [NewAccessList2,NewWaitList2]),
									failTrack(Sid,lists:delete(Tid,AgeList), ROList, NewWaitList2, Tail, lists:append(AbortList, [Tid]), TmpAccessList);
								{false,Sidtmp} ->
									if 
										Sid =:= Sidtmp -> 
											%io:format("W False~n"),
											io:format("~s aborted~n", [Tid]),
											NewWaitList = deleteElement(Tid,WaitList,[]),
											NewAccessList = deleteElement(Tid, AccessList,[]),
											rpc:call(db@localhost, adb_db, release,[Tid]),
											[ NewAccessList2| [ NewWaitList2 ] ] = checkWaitList(AgeList, ROList,NewAccessList, NewWaitList,AbortList, []),
											failTrack(Sid,lists:delete(Tid,AgeList), ROList, NewWaitList2, Tail, lists:append(AbortList, [Tid]), TmpAccessList);
											
										true ->
											failTrack(Sid,AgeList, ROList, WaitList, Tail, AbortList, lists:append(TmpAccessList, [Head]))
									end
							end;
						 [{_,ValId}]->
 							[{Tid, ValId}] = Detail,
							%io:format("---------~p~n", [ValId]),
 							case evenNum(ValId) of
 								{true} -> 
 									io:format("~s aborted~n", [Tid]),
 									NewWaitList = deleteElement(Tid,WaitList,[]),
 									NewAccessList = deleteElement(Tid, AccessList,[]),
 									rpc:call(db@localhost, adb_db, release,[Tid]),
									%io:format("R TRUE~n"),
 									[ NewAccessList2| [ NewWaitList2 ] ] = checkWaitList(AgeList, ROList,NewAccessList, NewWaitList, AbortList, []),
									
									
 									failTrack(Sid,lists:delete(Tid,AgeList), ROList, NewWaitList2, Tail, lists:append(AbortList, [Tid]), TmpAccessList);
 								{false,Sidtmp} ->
									%io:format("false ~p~n", [Sidtmp]),
									%io:format("false ~p~n", [Sid]),
 									if 
 										Sid =:= Sidtmp ->
 											%io:format("R FALSE~n"),
											io:format("~s aborted~n", [Tid]),
 											NewWaitList = deleteElement(Tid,WaitList,[]),
 											NewAccessList = deleteElement(Tid, AccessList,[]),
 											rpc:call(db@localhost, adb_db, release,[Tid]),
 											[ NewAccessList2| [ NewWaitList2 ] ] = checkWaitList(AgeList, ROList,NewAccessList, NewWaitList, AbortList, []),
 											failTrack(Sid,lists:delete(Tid,AgeList), ROList, NewWaitList2, Tail, lists:append(AbortList, [Tid]), TmpAccessList);
											
 										true ->
 											failTrack(Sid,AgeList, ROList, WaitList, Tail, AbortList, lists:append(TmpAccessList, [Head]))
									end
							end
					end
		end.	
