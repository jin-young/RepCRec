all: clean compile

compile:
	mkdir -p ./lib; \
	erlc -o ./lib ./src_erl/*.erl
	
clean:
	rm -rf ./lib/*
