-module(adb_client).
-export([start/0, start/1,beginT/1,beginRO/1,r/2,w/3,dump/0,dump/1,endT/1,fail/1,recover/1]).
            
start() ->
    io:format("***** NO INPUT FILE SPECIFIED. WATING FOR MANUAL OPERATION INPUT ******~n",[]).
    
start(Name) ->
    case file:open(Name, [read]) of 
        {ok, Device} -> for_each_line(Device, fun(X) -> parse(X) end)
    end.

  
parse(X) -> send_command(string:tokens(X, ";")).
    
% I strongly believe that there is better way to handle below stupid cases. 
% But I don't have enough space to prove it.
send_command([]) -> {ok};
send_command([H|TL]) -> 
    Cmd = string:strip(string:strip(string:strip(string:strip(H, both), both, $\n),both, $\t),both),%skip white space
    if length(Cmd) =:= 0 -> send_command(TL); % skip empty line
      true ->
         case re:run(Cmd, "^\/\/.*") of
            {match, _} -> send_command(TL); % skip comment line
            nomatch -> 
                io:format("~s: ~n", [Cmd]),
                case re:run(Cmd, "beginRO(.+)") of
                    {match,_} ->
                        [A]=string:tokens(Cmd,"beginRO()"),
                        beginRO(A),
                        send_command(TL);
                    nomatch -> 
                        case re:run(Cmd, "R(.+)") of 
                            {match, _} ->
                                [A,B]=string:tokens(Cmd,"R( , )"),
                                r(A,B),
                                send_command(TL);
                            nomatch ->
                                case re:run(Cmd, "W(.+)") of
                                    {match, _} ->
                                        [A,B,C]=string:tokens(Cmd,"W( , )"),
                                        w(A,B,C),
                                        send_command(TL);
                                    nomatch ->
                                        case re:run(Cmd, "dump()") of
                                            {match, _} ->
                                                trydump(string:tokens(Cmd,"dump()")),
                                                 %       dump(A),
                                                send_command(TL);
                                            nomatch ->
                                                case re:run(Cmd, "end(.+)") of
                                                    {match, _} ->
                                                        [A]=string:tokens(Cmd,"end()"),
                                                        endT(A),
                                                        send_command(TL);
                                                    nomatch ->
                                                        case re:run(Cmd, "fail(.+)") of
                                                            {match, _} ->
                                                                [A]=string:tokens(Cmd,"fail()"),
                                                                fail(A),
                                                                send_command(TL);
                                                            nomatch ->
                                                                case re:run(Cmd, "recover\(.+\)") of
                                                                    {match, _} ->
                                                                        [A]=string:tokens(Cmd,"recover()"),
                                                                        recover(A),
                                                                        send_command(TL);
                                                                    nomatch ->
                                                                        case re:run(Cmd, "begin(.+)") of
                                                                               {match,_} ->
                                                                                [A]=string:tokens(Cmd,"begin()"),
                                                                                beginT(A),
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
        end
    end.
                       
beginT(Tid) ->
    %io:format("~p~n",[rpc:call(tm@localhost, adb_tm, beginT, [Tid])]).
    %io:format("\tTransation ~p has been started.~n",[rpc:call(tm@localhost, adb_tm, beginT, [Tid])]).
   rpc:call(tm@localhost, adb_tm, beginT, [Tid]).
beginRO(Tid) ->
    %io:format("\tRead-Only transation ~p has been started.~n",[rpc:call(tm@localhost, adb_tm, beginRO, [Tid])]).
	rpc:call(tm@localhost, adb_tm, beginRO, [Tid]).
r(Tid, ValId) ->
    {A,B}=rpc:call(tm@localhost, adb_tm, r, [Tid, ValId]).
    %io:format("\tTransation ~p read a value of ~p~n",[A,B]),
    
w(Tid, ValId, Value) ->
    NewValue = case is_integer(Value) of
        true ->
            integer_to_list(Value);
        false ->
            Value
    end,
    {A,B,C}=rpc:call(tm@localhost, adb_tm, w, [Tid, ValId, NewValue]).
    %io:format("\tTransaction ~p update variable ~p with new value ~p~n",[A,B,C]).

dump() ->
    io:format("~n================ BEGIN ALL SITES DUMPING =================~n"),
    io:format("~p~n",[rpc:call(tm@localhost, adb_tm, dump, [])]),
    io:format("================ END ALL SITES DUMPING =================~n").
	%rpc:call(tm@localhost, adb_tm, dump, []).
	
dump(Id) ->%this is for dump(1) or dump(x1)
    ConvId = case is_integer(Id) of
        true ->
            integer_to_list(Id);
        false ->
            Id
    end,
    io:format("~n================ BEGIN DUMPING FOR ~p =================~n", [ConvId]),
    io:format("~p~n",[rpc:call(tm@localhost, adb_tm, dump, [ConvId])]),
    io:format("================ END DUMPING FOR ~p =================~n", [ConvId]).    
    %rpc:call(tm@localhost, adb_tm, dump, [ConvId]).
   
endT(Tid) ->
    %io:format("\tTransation ~p has been ended.~n",[rpc:call(tm@localhost, adb_tm, endT, [Tid])]).
	rpc:call(tm@localhost, adb_tm, endT, [Tid]).
fail(SiteId) ->
    ConvId = case is_integer(SiteId) of
        true ->
            integer_to_list(SiteId);
        false ->
            SiteId
    end,
	rpc:call(tm@localhost, adb_tm, fail, [ConvId]).
    %io:format("\tSite ~p has been failed.~n",[rpc:call(tm@localhost, adb_tm, fail, [ConvId])]).
    
recover(SiteId) ->
    ConvId = case is_integer(SiteId) of
        true ->
            integer_to_list(SiteId);
        false ->
            SiteId
    end,
	rpc:call(tm@localhost, adb_tm, recover, [ConvId]).
    %io:format("\tSite ~p has been recovered.~n",[rpc:call(tm@localhost, adb_tm, recover, [ConvId])]).

trydump([]) ->dump();
trydump([H]) ->dump(H).

for_each_line(Device, Proc) ->
    case io:get_line(Device, "") of
        eof  -> file:close(Device);
        {error, Reason} -> elrang:error(Reason);
        Line -> Proc(Line),
                for_each_line(Device, Proc)
    end.
