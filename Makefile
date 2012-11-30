all: clean compile

compile:
	mkdir -p ./bin; \
	erlc -o ./bin ./src_erl/*.erl
	
clean:
	rm -rf ./bin/*
