#!/bin/bash

erl -pa ./bin -sname tm@localhost -noshell -s adb_tm start
