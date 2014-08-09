all: binaries

binaries: dist/bin/client-m2mp dist/bin/receiver-m2mp

dist/bin/client-m2mp: client-m2mp/*.go
	cd client-m2mp ; go get -v ; go build -v
	mkdir -p dist/bin
	mv client-m2mp/client-m2mp dist/bin/client-m2mp

dist/bin/receiver-m2mp: receiver-m2mp/*.go
	cd receiver-m2mp; go get -v ; go build -v
	mkdir -p dist/bin
	mv receiver-m2mp/receiver-m2mp dist/bin/receiver-m2mp

install: binaries
	# Binaries
	mkdir -p $(DESTDIR)/usr/bin
	cp dist/bin/* $(DESTDIR)/usr/bin

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

package: binaries
	dpkg-buildpackage -b -us -uc
	mkdir -p dist/package
	mv ../*.deb dist/package/
	rm ../*.changes

test_package:
	make clean
	make package
	sudo dpkg -r m2mp || echo "No M2MP package"
	sudo dpkg -i dist/package/*.deb

clean:
	[ ! -d dist ] || rm -R dist

