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

package: binaries
	mkdir -p dist/package
	cd package; dpkg-buildpackage -b -us -uc
	mv *.deb dist/package

install: binaries
	# Binaries
	mkdir -p $(DESTDIR)/usr/bin
	cp bin/* $(DESTDIR)/usr/bin

	# Logging
	mkdir -p $(DESTDIR)/var/log/m2mp $(DESTDIR)/etc/logrotate.d
	cp package/logrotate.d $(DESTDIR)/etc/logrotate.d

clean:
	[ ! -d dist ] || rm -R dist

