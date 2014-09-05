all: binaries

binaries: dist/bin/client-m2mp dist/bin/receiver-m2mp dist/bin/m2mp-storage dist/bin/converter-m2mp dist/bin/receiver-alip

dist/bin/client-m2mp: client-m2mp/*.go
	cd client-m2mp ; go get -v ; go build -v
	mkdir -p dist/bin
	mv client-m2mp/client-m2mp dist/bin/client-m2mp

dist/bin/receiver-m2mp: receiver-m2mp/*.go
	cd receiver-m2mp; go get -v ; go build -v
	mkdir -p dist/bin
	mv receiver-m2mp/receiver-m2mp dist/bin/receiver-m2mp

dist/bin/converter-m2mp: converter-m2mp/*.go
	cd converter-m2mp; go get -v ; go build -v
	mkdir -p dist/bin
	mv converter-m2mp/converter-m2mp dist/bin/converter-m2mp

dist/bin/receiver-alip: receiver-alip/*.go
	cd receiver-alip; go get -v ; go build -v
	mkdir -p dist/bin
	mv receiver-alip/receiver-alip dist/bin/receiver-alip

dist/bin/m2mp-storage: m2mp-storage/*.go
	cd m2mp-storage; go get -v ; go build -v
	mkdir -p dist/bin
	mv m2mp-storage/m2mp-storage dist/bin/m2mp-storage

install: binaries
	# Binaries
	mkdir -p $(DESTDIR)/usr/bin
	cp dist/bin/* $(DESTDIR)/usr/bin
	cp package/bin/* $(DESTDIR)/usr/bin
	chmod a+rx $(DESTDIR)/usr/bin/*

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

package: binaries
	dpkg-buildpackage -b -us -uc
	mkdir -p dist/package
	mv ../*.deb dist/package/
	rm ../*.changes

test_package_local:
	make clean
	make package
	sudo dpkg -i dist/package/*.deb

clean:
	[ ! -d dist ] || rm -R dist

test_package_remote:
	make package
	rsync -avz --progress dist/package/*.deb $(TARGET):package.deb
	ssh $(TARGET) dpkg -i package.deb
