VERSION=4.0
PACKAGE=libpve-storage-perl
PKGREL=76

DESTDIR=
PREFIX=/usr
BINDIR=${PREFIX}/bin
SBINDIR=${PREFIX}/sbin
MANDIR=${PREFIX}/share/man
DOCDIR=${PREFIX}/share/doc/${PACKAGE}
MAN1DIR=${MANDIR}/man1/
BASHCOMPLDIR=${PREFIX}/share/bash-completion/completions/

export PERLDIR=${PREFIX}/share/perl5

#ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
ARCH=all
GITVERSION:=$(shell cat .git/refs/heads/master)

DEB=${PACKAGE}_${VERSION}-${PKGREL}_${ARCH}.deb

# this require package pve-doc-generator
export NOVIEW=1
include /usr/share/pve-doc-generator/pve-doc-generator.mk

all: ${DEB}

.PHONY: dinstall
dinstall: deb
	dpkg -i ${DEB}

pvesm.bash-completion:
	perl -I. -T -e "use PVE::CLI::pvesm; PVE::CLI::pvesm->generate_bash_completions();" >$@.tmp
	mv $@.tmp $@

.PHONY: install
install: pvesm.1 pvesm.bash-completion
	install -d ${DESTDIR}${SBINDIR}
	install -m 0755 pvesm ${DESTDIR}${SBINDIR}
	make -C PVE install
	install -d ${DESTDIR}/usr/share/man/man1
	install -m 0644 pvesm.1 ${DESTDIR}/usr/share/man/man1/
	gzip -9 -n ${DESTDIR}/usr/share/man/man1/pvesm.1
	install -m 0644 -D pvesm.bash-completion ${DESTDIR}${BASHCOMPLDIR}/pvesm

.PHONY: deb
deb: ${DEB}
${DEB}:
	rm -rf debian
	mkdir debian
	make DESTDIR=${CURDIR}/debian install
	perl -I. -T -e "use PVE::CLI::pvesm; PVE::CLI::pvesm->verify_api();"
	install -d -m 0755 debian/DEBIAN
	sed -e s/@@VERSION@@/${VERSION}/ -e s/@@PKGRELEASE@@/${PKGREL}/ -e s/@@ARCH@@/${ARCH}/ <control.in >debian/DEBIAN/control
	install -D -m 0644 copyright debian/${DOCDIR}/copyright
	install -m 0644 changelog.Debian debian/${DOCDIR}/
	install -m 0644 triggers debian/DEBIAN
	gzip -9 -n debian/${DOCDIR}/changelog.Debian
	echo "git clone git://git.proxmox.com/git/pve-storage.git\\ngit checkout ${GITVERSION}" > debian/${DOCDIR}/SOURCE
	fakeroot dpkg-deb --build debian
	mv debian.deb ${DEB}
	rm -rf debian
	lintian ${DEB}

.PHONY: clean
clean:
	make cleanup-docgen
	rm -rf debian *.deb ${PACKAGE}-*.tar.gz dist *.1 *.tmp pvesm.bash-completion
	find . -name '*~' -exec rm {} ';'

.PHONY: distclean
distclean: clean


.PHONY: upload
upload: ${DEB}
	tar cf - ${DEB} | ssh repoman@repo.proxmox.com -- upload --product pve --dist jessie

