TARGET=client-alip

BINARIES=$(TARGET) $(TARGET).exe

FILES=$(shell ls *.go)

all: $(BINARIES)

clean:
	rm $(BINARIES) *.log || printf ""

$(BINARIES): $(FILES)
	#export GOROOT=/usr/local/go
	#export PATH=$PATH:$GOROOT/bin
	#export GOPATH=`pwd`/go
	go get -d -v
	go build -v -o $(TARGET)
	env GOARCH=386 GOOS=windows go get -d -v
	env GOARCH=386 GOOS=windows go build -v -o $(TARGET).exe

run: $(TARGET)
	./$(TARGET)
