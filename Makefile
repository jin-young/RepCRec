all: clean compile

compile:
	mkdir -p ./lib; \
	erlc -o ./lib ./src/*.erl
	
clean:
	rm -rf ./lib/*
