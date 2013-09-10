LIBEVENT_HOME=/usr

# Remove NDEBUG define to trigger asserts
CPPFLAGS+=-O2 -I. -DNDEBUG -Wall -Wno-sign-compare -Wno-format -Wno-unused -g -I${LIBEVENT_HOME}/include -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -DRECIP -DRECIP_TYPE=3
LDFLAGS+=-levent -lstdc++ -Lext/recip

all: swift-dynamic

swift: swift.o sha1.o compat.o sendrecv.o send_control.o hashtree.o bin.o binmap.o channel.o transfer.o httpgw.o statsgw.o cmdgw.o avgspeed.o avail.o storage.o zerostate.o zerohashtree.o ext/recip/base.o ext/recip/winner.o
	#nat_test.o

swift-static: swift
	g++ ${CPPFLAGS} -o swift *.o ${LDFLAGS} -static -lrt
	strip swift
	touch swift-static

swift-dynamic: swift
	g++ ${CPPFLAGS} -o swift *.o ext/recip/*.o ${LDFLAGS} -L${LIBEVENT_HOME}/lib
	touch swift-dynamic

clean:
	rm *.o swift swift-static swift-dynamic2>/dev/null

.PHONY: all clean swift swift-static swift-dynamic
