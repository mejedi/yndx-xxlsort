CPPFLAGS +=-std=c++11 -stdlib=libc++
LDLIBS = -lc++

xxlsort: xxlsort.o util.o

binarizer: binarizer.o util.o

clean:
	rm -f *.o xxlsort binarizer
