-module(adb_transaction).

start(TransId) ->
     register(TransId, spawn(fun() -> loop() end)).
     
read
    
loop(Tm) ->
    receive
        {read, {VariableId}} ->
        
