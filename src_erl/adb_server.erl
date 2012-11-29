-module(adb_server).
-export([start/0, unavailable/0, available/0,read/2,write/2,commit/1]).

start() -> 
	% create a new 10 sites
	% set all sites available
	siteInit()
	spawn(fun() -> loop() end)).

indexInit() ->
	

siteInit() ->
	% site 1 - 10
	% x index 1 - 20
	Site1 = {1, }
	Site2 = {}
	Site3 = { }
	Site4 = { }
	Site5 = { }

loop() ->
	receive
	{ From, {Read, siteId}} ->
		
			
