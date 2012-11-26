-module(adb_tm).
-export([start/0, stop/0, beginT/1, endT/1, write/3, read/2, beginRT/1, dump/0, dump/1, fail/1, recover/1).

start() ->
     register(adb_tm, spawn(fun() ->
				     loop() end)).
beginT(TId) ->
    rpc({beginT, TId}).

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
	    if lookupT(TransId) == undefined ->
		    NewTransId = spawn(fun() -> adb_tran:start()), registerT(newTransId), From ! {ok};
	    loop();
	{From, {endT, TransId}} ->
	    loop();
	{From, {write, TransId, Variable, Value}} ->
	    loop();
	{From, {read, TransId, Variable}} ->
	    loop();
	{From, {beginRT, TransId}} ->
	    loop();
	{From, {dump}} ->
	    loop();
	{From, {dump, SiteId}} ->
	    loop();
        {From, {fail, SiteId}} ->
	    loop();
	{From, {recover, SiteId}} ->
	    loop()
    end.
