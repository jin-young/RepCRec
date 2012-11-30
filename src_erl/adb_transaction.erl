-module(adb_transaction).

start(TransId) ->
     register(TransId, spawn(fun() -> loop() end)).
     
loop() ->
    receive
        {From, {read, VariableId}} ->
            loop()
    end.
        
