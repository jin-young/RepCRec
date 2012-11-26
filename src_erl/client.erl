-module(client).
-export([start/1, for_each_line_in_file/3, gen/1]).

%1> A = for_each_line_in_file("complex.erl",
%1>   fun(X, Count) -> io:fwrite("~10B: ~s", [Count, X]),
%1>       Count + 1 end, [read], 0).

start(Name) -> for_each_line_in_file(Name, fun(X) ->  gen(X) end, [read]).

gen(X) ->
	%io:fwrite("~s", [X]).
	rpc:call(tm@localhost, kvs, beginTransaction, [X]).
	
for_each_line_in_file(Name, Proc, Mode) ->
	{ok, Device} = file:open(Name, Mode),
	for_each_line(Device, Proc).

for_each_line(Device, Proc) ->
	case io:get_line(Device, "") of
		eof  -> file:close(Device);
		Line -> Proc(Line),
		for_each_line(Device, Proc)
	end.
