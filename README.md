RepCRec
=======

This is Replicated Concurrency Control and Recovery, a.k.a. RepCRec, project for 2012 NYU Computer Science Advanced Database Systems class final project.

TEAM MEMBER:
 * Suphannee Sivakorn (ss6330@nyu.edu)
 * SooBok Shin (ss6321@nyu.edu)
 * JinYoung Heo (jyh300@nyu.edu)

REQUIREMEMT:

 * erlang : Erlang programing langugage version at least R14B04

HOW TO COMPILE:

 * make : all erlang object file (beam) will be generated in ./libs 

HOW TO RUN:

 * tm: ./bin/tm : starts Transaction Manager. Run before start client
 * db: ./bin/db : starts DB sites. Run before start client
 
 
 * client: ./bin/client [testFilePathAndName]
 
    ex) ./bin/client ./test/projectsampletests.txt  : read input file and run it
    ex) ./bin/client  : waiting for manual operation input
    
COMMAND EXAMPLE FOR MANUAL OPERATION

Client:
  * adb_client:beginT("T1")
  * adb_client:beginRO("T2")
  * adb_client:r("T1", "x2")
  * adb_client:w("T2", "x19", 3000) or adb_client:w("T2", "x19", "3000")
  * adb_client:dump()
  * adb_client:dump("x1")
  * adb_client:dump(1) or adb_client("1")
  * adb_client:end("T2") 
  * adb_client:fail(3) or adb_client:fail("3")
  * adb_client:recover(3) or adb_client:recover("3")
  
TransactionManager:

Site:
