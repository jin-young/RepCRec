-module(adb_client).
-export([start/1]).
			
start(Name) ->
	case file:open(Name, [read]) of 
	    {ok, Device} -> for_each_line(Device, fun(X) -> parse(X) end)
    end.

  
parse(X) -> send_command(string:tokens(X, ";")).
	
% I strongly believe that there is better way to handle below stupid cases. 
% But I don't have enough space to prove it.
send_command([]) -> {ok};
send_command([H|TL]) -> 
    Cmd = string:strip(string:strip(H, both), right, $\n),
    if length(Cmd) =:= 0 -> send_command(TL); % skip empty line
      true ->
         case re:run(Cmd, "^\/\/.*") of
            {match, _} -> send_command(TL); % skip comment line
            nomatch -> 
                io:format("~s~n", [Cmd]),
                case re:run(Cmd, "begin(.+)") of
                    {match, Captured} -> 
                        beginT("T1"),
                        send_command(TL);
                    nomatch -> 
                        case re:run(Cmd, "R\(.+\)") of 
                            {match, Captures} ->
                                read("T1", "v1"),
                                send_command(TL);
                            nomatch ->
                                case re:run(Cmd, "W\(.+\)") of
                                    {match, Captures} ->
                                        write("T1", "v1", 5),
                                        send_command(TL);
                                    nomatch ->
                                        case re:run(Cmd, "dump\(\)") of
                                            {match, Captures} ->
                                                dump(),
                                                send_command(TL);
                                            nomatch ->
                                                case re:run(Cmd, "end\(.+\)") of
                                                    {match, Captures} ->
                                                        endT("T1"),
                                                        send_command(TL);
                                                    nomatch ->
                                                        case re:run(Cmd, "fail\(.+\)") of
                                                            {match, Captures} ->
                                                                fail(5),
                                                                send_command(TL);
                                                            nomatch ->
                                                                case re:run(Cmd, "recover\(.+\)") of
                                                                    {match, Captures} ->
                                                                        recover(5),
                                                                        send_command(TL);
                                                                    nomatch ->
                                                                        erlang:error({invalidInstructionError, Cmd})
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
    rpc:call(tm@localhost, adb_tm, beginT, [Tid]).
    
read(Tid, ValId) ->
    rpc:call(tm@localhost, adb_tm, read, [Tid, ValId]).
    
write(Tid, ValId, Value) ->
    rpc:call(tm@localhost, adb_tm, write, [Tid, ValId, Value]).

dump() ->
    rpc:call(tm@localhost, adb_tm, dump, []).
    
endT(Tid) ->
    rpc:call(tm@localhost, adb_tm, dump, [Tid]).    
    
fail(SiteId) ->
    io:format("Not Implemented Yet\n").
    
recover(SiteId) ->
    io:format("Not Implemented Yet\n").

for_each_line(Device, Proc) ->
    case io:get_line(Device, "") of
	    eof  -> file:close(Device);
	    {error, Reason} -> elrang:error(Reason);
	    Line -> Proc(Line),
	            for_each_line(Device, Proc)
    end.
