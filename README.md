RepCRec
=======

This is Replicated Concurrency Control and Recovery, a.k.a. RepCRec, project for 2012 NYU Computer Science Advanced Database Systems class final project.

TEAM MEMBER:
 * Suphannee Sivakorn (suphannee.s@nyu.edu)
 * SooBok Shin (ss6321@nyu.edu)
 * JinYoung Heo (jinyoung@nyu.edu)

REQUIREMEMT:

 * erlang : Erlang programing langugage version at least R14B04

HOW TO COMPILE:

 * make : all erlang object file (beam) will be generated in ./libs 

HOW TO RUN:

 * tm: ./bin/tm : run before start client
 * db: ./bin/db : run before start client
 
 
 * client: ./bin/client [testFilePathAndName]
 
    ex) ./bin/client ./test/projectsampletests.txt  : read input file and run it
    ex) ./bin/client  : waiting for manual operation input
