-module(adb_client).
-export([start/1,beginT/1,beginRO/1,r/2,w/3,dump/0,dump/1,endT/1,fail/1,recover/1]).
			
start(Name) ->
	case file:open(Name, [read]) of 
	    {ok, Device} -> for_each_line(Device, fun(X) -> parse(X) end)
    end.

  
parse(X) -> send_command(string:tokens(X, ";")).
	
% I strongly believe that there is better way to handle below stupid cases. 
% But I don't have enough space to prove it.
send_command([]) -> {ok};
send_command([H|TL]) -> 
    Cmd = string:strip(string:strip(string:strip(H, both), both, $\n),both, $\t),%skip white space
    if length(Cmd) =:= 0 -> send_command(TL); % skip empty line
      true ->
         case re:run(Cmd, "^\/\/.*") of
            {match, _} -> send_command(TL); % skip comment line
            nomatch -> 
                io:format("~s~n", [Cmd]),
                case re:run(Cmd, "beginRO(.+)") of
       		      	{match,Captures} ->
				[A]=string:tokens(Cmd,"beginRO()"),
				beginRO(A),
		                send_command(TL);
			nomatch -> 
		case re:run(Cmd, "R(.+)") of 
                        {match, Captures} ->
				[A|B]=string:tokens(Cmd,"R(,)"),
                	        r(A,B),
                                send_command(TL);
                        nomatch ->
       	        case re:run(Cmd, "W(.+)") of
                        {match, Captures} ->
				[A,B,C]=string:tokens(Cmd,"W(,)"),
                                w(A,B,C),
                                send_command(TL);
                        nomatch ->
       	        case re:run(Cmd, "dump()") of
                        {match, Captures} ->
				trydump(string:tokens(Cmd,"dump()")),
			
                         %       dump(A),
        	                send_command(TL);
                        nomatch ->
       	        case re:run(Cmd, "end(.+)") of
                        {match, Captures} ->
				[A]=string:tokens(Cmd,"end()"),
                                endT(A),
                                send_command(TL);
                        nomatch ->
        	case re:run(Cmd, "fail(.+)") of
                        {match, Captures} ->
				[A]=string:tokens(Cmd,"fail()"),
                                fail(A),
                                send_command(TL);
                        nomatch ->
                case re:run(Cmd, "recover\(.+\)") of
                        {match, Captures} ->
				[A]=string:tokens(Cmd,"recover()"),
                                recover(A),
                                send_command(TL);
                        nomatch ->
		case re:run(Cmd, "begin(.+)") of
                   	{match,Captures} ->
				[A]=string:tokens(Cmd,"begin()"),
				beginT(A),
                	        send_command(TL);
                        nomatch ->                                              				erlang:error({invalidInstructionError, Cmd})
		end
		end						
                end
                end
                end
                end 
                end
                end
                end
    		end.
			           
beginT(Tid) ->
	io:format("~s~n",[rpc:call(tm@localhost, adb_tm, beginT, [Tid])]).
   
beginRO(Tid) ->
    io:format("~s~n",[rpc:call(tm@localhost, adb_tm, beginRO, [Tid])]).

r(Tid, ValId) ->
    io:format("~s~n",[rpc:call(tm@localhost, adb_tm, read, [Tid, ValId])]).
    
w(Tid, ValId, Value) ->
    io:format("~s~n",[rpc:call(tm@localhost, adb_tm, write, [Tid, ValId, Value])]).

dump() ->
    io:format("~s~n",[rpc:call(tm@localhost, adb_tm, dump, [])]).

dump(Tid) ->%this is for dump(1) or dump(x1)
	io:format("~s~n",[rpc:call(tm@localhost, adb_tm, dump, [Tid])]).
   
endT(Tid) ->
    io:format("~s~n",[rpc:call(tm@localhost, adb_tm, endT, [Tid])]).    

fail(SiteId) ->
    io:format("~s~n",[rpc:call(tm@localhost, adb_tm, fail, [SiteId])]).
    
recover(SiteId) ->
  	io:format("~s~n",[rpc:call(tm@localhost, adb_tm, recover, [SiteId])]).

trydump([]) ->dump();
trydump([H]) ->dump(H).

for_each_line(Device, Proc) ->
    case io:get_line(Device, "") of
	    eof  -> file:close(Device);
	    {error, Reason} -> elrang:error(Reason);
	    Line -> Proc(Line),
	            for_each_line(Device, Proc)
    end.
