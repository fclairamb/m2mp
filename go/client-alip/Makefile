TARGET=client-alip


all: $(TARGET) $(TARGET).exe


$(TARGET) $(TARGET).exe: $(FILES)
	#export GOROOT=/usr/local/go
	#export PATH=$PATH:$GOROOT/bin
	#export GOPATH=`pwd`/go
	go get -d -v
	go build -v -o $(TARGET)
	env GOARCH=386 GOOS=windows go get -d -v
	env GOARCH=386 GOOS=windows go build -v -o $(TARGET).exe
