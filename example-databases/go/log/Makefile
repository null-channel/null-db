BINARY_NAME=nulldb-go

build:
	cd cmd && go build -o $(BINARY_NAME) 

run: build
	cd cmd &&./$(BINARY_NAME)

clean:
	rm cmd/$(BINARY_NAME)