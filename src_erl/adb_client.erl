-module(adb_client).
-export([start/1]).
			
start(Name) ->
	case file:open(Name, [read]) of 
	    {ok, Device} -> for_each_line(Device, fun(X) -> parse(X) end);
	    _ -> 
    end.
    
parse(X) ->
	Split=re:split([X],";",[{return,list},trim]),
	send_command(Split).
	
send_command([]) -> {ok};
send_command([H|TL]) -> io:format("~p~n", [H]),
			            send_command(TL).	
			            
beginT(Tid) ->
    rpc:call(tm@localhost, adb_tm, beginT, [Tid]).
    
read(Tid, ValId) ->
    rpc:call(tm@localhost, adb_tm, read, [Tid, ValId]).
    
write(Tid, ValId, Value) ->
    rpc:call(tm@localhost, adb_tm, write, [Tid, ValId, Value]).

dump() ->
    rpc:call(tm@localhost, adb_tm, dump, []).
    
endT(Tid) ->
    rpc:call(tm@localhost, adb_tm, dump, [Tid]).    

for_each_line(Device, Proc) ->
	case io:get_line(Device, "") of
		eof  -> file:close(Device);
		Line -> Proc(Line),
		        for_each_line(Device, Proc)
	end.
