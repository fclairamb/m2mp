PACKAGE_VERSION=$(shell dpkg-parsechangelog | grep Version: | cut -d' ' -f2-)
VERSION_FILE=m2mp-version/version.go

ifeq "$(BUILD_NUMBER)" ""
	VERSION=$(PACKAGE_VERSION)
else
	VERSION=$(PACKAGE_VERSION)-$(BUILD_NUMBER)
endif

all: binaries win-binaries

create-version: $(VERSION_FILE)

$(VERSION_FILE): $(shell find -not -path "*/m2mp-version/*" -name "*.go") Makefile
	printf 'package m2mp_version\n\nconst VERSION = "$(VERSION)"\n' >$@

binaries: dist/bin/client-m2mp dist/bin/receiver-m2mp dist/bin/m2mp-storage dist/bin/converter-m2mp dist/bin/receiver-alip dist/bin/client-alip

win-binaries: dist/win/client-alip.exe

dist/win/client-alip.exe: create-version client-alip/*.go 
	mkdir -p dist/win
	cd client-alip && make client-alip.exe
	cp client-alip/client-alip.exe dist/win

dist/bin/client-m2mp: create-version client-m2mp/*.go
	cd client-m2mp ; go get -v ; go build -v
	mkdir -p dist/bin
	mv client-m2mp/client-m2mp dist/bin/client-m2mp

dist/bin/receiver-m2mp: create-version receiver-m2mp/*.go
	cd receiver-m2mp; go get -v ; go build -v
	mkdir -p dist/bin
	mv receiver-m2mp/receiver-m2mp dist/bin/receiver-m2mp

dist/bin/converter-m2mp: create-version converter-m2mp/*.go
	cd converter-m2mp; go get -v ; go build -v
	mkdir -p dist/bin
	mv converter-m2mp/converter-m2mp dist/bin/converter-m2mp

dist/bin/client-alip: create-version client-alip/*.go
	cd client-alip ; go get -v ; go build -v
	mkdir -p dist/bin
	mv client-alip/client-alip dist/bin/client-alip

dist/bin/receiver-alip: create-version receiver-alip/*.go
	cd receiver-alip; go get -v ; go build -v
	mkdir -p dist/bin
	mv receiver-alip/receiver-alip dist/bin/receiver-alip

dist/bin/m2mp-storage: create-version m2mp-storage/*.go
	cd m2mp-storage; go get -v ; go build -v
	mkdir -p dist/bin
	mv m2mp-storage/m2mp-storage dist/bin/m2mp-storage

install: binaries
	# Binaries
	mkdir -p $(DESTDIR)/usr/bin
	cp dist/bin/* $(DESTDIR)/usr/bin
	cp package/bin/* $(DESTDIR)/usr/bin
	chmod a+rx $(DESTDIR)/usr/bin/*

	# Config
	mkdir -p $(DESTDIR)/etc/m2mp
	cp m2mp-storage/m2mp-storage.conf $(DESTDIR)/etc/m2mp
	cp receiver-m2mp/receiver-m2mp.conf $(DESTDIR)/etc/m2mp
	cp converter-m2mp/converter-m2mp.conf $(DESTDIR)/etc/m2mp
	cp receiver-alip/receiver-alip.conf $(DESTDIR)/etc/m2mp

	# Data dir
	mkdir -p $(DESTDIR)/var/lib/m2mp

	# Logging
	mkdir -p $(DESTDIR)/var/log/m2mp $(DESTDIR)/etc/logrotate.d
	cp package/logrotate.d/* $(DESTDIR)/etc/logrotate.d

	# Startup scripts
	mkdir -p $(DESTDIR)/etc/supervisor/conf.d
	cp package/supervisor.d/*.conf $(DESTDIR)/etc/supervisor/conf.d

	# CQL install commands
	mkdir -p $(DESTDIR)/usr/share/m2mp/cql
	cp ../cql/* $(DESTDIR)/usr/share/m2mp/cql

	# NSQ setup (this is temporary, it should be in an other package)
	mkdir -p $(DESTDIR)/var/lib/nsq $(DESTDIR)/var/log/nsq/topics

clean:
	[ ! -d dist ] || rm -R dist

package: binaries
	dpkg-buildpackage -b -us -uc
	mkdir -p dist/package
	mv ../*.deb dist/package/
	rm ../*.changes

test_package_local:
	make clean
	make package
	sudo dpkg -i dist/package/*.deb

test_package_remote:
	make package
	rsync -avz --progress dist/package/*.deb $(TARGET):m2mp.deb
	ssh $(TARGET) dpkg -i m2mp.deb
