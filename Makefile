all: compile run_tm run_client

compile:
	mkdir -p ./bin; \
	erlc -o ./bin ./src_erl/*.erl

clean:
	rm -rf ./bin/*
