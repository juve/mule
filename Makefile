BSDDB3=bsddb3-5.0.0

all: lib/bsddb3

lib/bsddb3:
	cd externals; \
	tar xzvf $(BSDDB3).tar.gz; \
	cd $(BSDDB3); \
	python setup.py build; \
	mv build/lib.*/bsddb3 ../../lib

clean:
	rm -rf lib/bsddb3 externals/$(BSDDB3)

