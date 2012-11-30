-module(adb_tm).
%-export([start/0, stop/0, beginT/1, endT/1, write/3, read/2, beginRT/1, dump/0, dump/1, fail/1, recover/1).
-export([start/0, beginT/1, endT/1, write/3, read/2, beginRO/1, dump/0, dump/1, fail/1, recover/1]).

start() ->
     register(adb_tm, spawn(fun() ->
				     loop() end)).
beginT(Tid) ->
	rpc({beginT, Tid}).
    %io:fwrite("beginT ~s~n", [Tid]).
	%rpc:call(client@localhost, adb_tm, beginT, [Tid]).
beginRO(Tid) ->
	rpc({beginT, Tid}).

dump() ->
	io:fwrite("dump ~n").
	
dump(Tid) ->
	io:fwrite("dump ~s~n", [Tid]).
	
fail(Sid) ->
	io:fwrite("fail ~s~n", [Sid]).

recover(Sid) ->
	io:fwrite("recover ~s~n", [Sid]).
	
read(Tid, ValId) ->
	io:fwrite("read ~s~n", [Tid]).

write(Tid, ValId, Value) ->
	rpc({w, {Tid, ValId, Value}}).

endT(Tid) ->
	io:fwrite("end ~s~n", [Tid]).
	
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
	{From, {endT, TransId}} ->
		From ! {adb_tm, TransId},
	    loop();
	{From, {w, {TransId, Variable, Value}}} ->
		From ! {adb_tm, {TransId, Variable, Value}},
	    loop();
	{From, {read, {TransId, Variable}}} ->
		From ! {adb_tm, {TransId, Variable}},
	    loop();
	{From, {beginRO, TransId}} ->
		From ! {adb_tm, TransId},
	    loop();
	{From, {dump}} ->
		From ! {adb_tm, "dumpppp"},
	    loop();
	{From, {dump, SiteId}} ->
		From ! {adb_tm, SiteId},
	    loop();
    {From, {fail, SiteId}} ->
		From ! {adb_tm, SiteId},
	    loop();
	{From, {recover, SiteId}} ->
		From ! {adb_tm, SiteId},
	    loop()
    end.
