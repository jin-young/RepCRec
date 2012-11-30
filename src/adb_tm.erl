-module(adb_tm).
%-export([start/0, stop/0, beginT/1, endT/1, write/3, read/2, beginRT/1, dump/0, dump/1, fail/1, recover/1).
-export([start/0, beginT/1, endT/1, w/3, r/2, beginRO/1, dump/0, dump/1, fail/1, recover/1]).

start() ->
     register(adb_tm, spawn(fun() ->
				     loop() end)).
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

loop() ->
    receive
	{From, {beginT, TransId}} ->
		
		From ! {adb_tm, TransId},
	    %if lookupT(TransId) == undefined ->
		    %NewTransId = spawn(fun() -> adb_tran:start()), registerT(newTransId), From ! {ok};
	    loop();
	{From, {endT, Tid}} ->
		From ! {adb_tm, Tid},
	    loop();
	{From, {w, {Tid, ValId, Value}}} ->
		From ! {adb_tm, {Tid, ValId, Value}},
	    loop();
	{From, {r, {Tid, ValId}}} ->
		From ! {adb_tm, {Tid, ValId}},
	    loop();
	{From, {beginRO, Tid}} ->
		From ! {adb_tm, Tid},
	    loop();
	{From, {dump}} ->
		From ! {adb_tm, dump},
	    loop();
	{From, {dump, Sid}} ->
		From ! {adb_tm, Sid},
	    loop();
    {From, {fail, Sid}} ->
		From ! {adb_tm, Sid},
	    loop();
	{From, {recover, Sid}} ->
		From ! {adb_tm, Sid},
	    loop()
    end.
