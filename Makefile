all: compile run_tm run_client

compile:
	mkdir -p ./bin; \
	erlc -o ./bin ./src_erl/*.erl
	
run_tm: compile
	erl -pa ./bin -sname tm@localhost -noshell -s adb_tm start
	
run_db: compile
	erl -pa ./bin -sname db@localhost -noshell -s adb_db start	
	
run_cl: compile
	erl -pa ./bin -sname tm@localhost -s adb_client start	
    
clean:
	rm -rf ./bin/*
