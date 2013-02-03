/*
 * Estimating an effect of various micro optimizations on the
 * performance of a standard sorting algorithm.
 *
 * Sample output:
 *

allocated 2147483712 bytes (2.000 GB)
Legend:
        ptr - sorting array of pointers
        offset - sorting array of offsets (smaller elements)
        prefixX - array elements include X bytes prefix of a key
        perfect-prefixX - acting as if prefix comparison alone is enough
    0:33.670965   ptr
    0:33.122159   offset
    0:09.144016   prefix8
    0:08.886393   perfect-prefix8
    0:09.698536   offset-prefix4
    0:08.608928   offset-perfect-prefix4
    0:09.129550   offset-prefix12
    0:09.671609   offset-perfect-prefix12

 *
 */

#include <sys/mman.h>
#include <sys/time.h>
#include <err.h>
#include <inttypes.h>
#include <algorithm>
#include <stdio.h>
#include <openssl/md5.h>


struct data_elem
{
    unsigned char key[64];
};


void init_data_elem(data_elem &e, size_t ordinal)
{
    memset(e.key, 0, sizeof e.key);
    MD5((const unsigned char *)&ordinal, sizeof ordinal, e.key);
}


struct benchmark
{
    static void *mem;
    static size_t memsz;
    static data_elem *first;
    static size_t n;

    static void init(size_t _n, size_t misalign = 0)
    {
        if (mem) {
            if (munmap(mem, memsz)==-1) {
                err(EXIT_FAILURE, "munmap");
            }
            mem = NULL;
        }

        memsz = (_n + 1) * sizeof (data_elem);
        mem = mmap(NULL, memsz, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
        if (!mem) {
            err(EXIT_FAILURE, "mmap");
        }
        first = (data_elem *)((uintptr_t)mem + misalign % sizeof(data_elem));
        n = _n;

        for (size_t i=0; i<n; i++) {
            init_data_elem(*(first+i), i);
        }

        printf("allocated %zu bytes (%.3f GB)\n", memsz, (float)memsz/1024/1024/1024);
    }

    template <typename Traits>
    static void run(const char *label, const Traits &)
    {
        typedef typename Traits::sortel sortel;
        size_t auxsz = n * sizeof(sortel);
        sortel *aux = (sortel *)mmap(NULL, auxsz, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);

        if (!aux) {
            err(EXIT_FAILURE, "mmap");
        }

        for (size_t i=0; i<n; i++) {
            aux[i].init(*(first+i));
        }

        timeval start;
        if (gettimeofday(&start, NULL)==-1) {
            err(EXIT_FAILURE, "gettimeofday");
        }

        std::sort(aux, aux+n);

        timeval end;
        if (gettimeofday(&end, NULL)==-1) {
            err(EXIT_FAILURE, "gettimeofday");
        }

        timeval delta;
        delta.tv_sec = end.tv_sec - start.tv_sec;
        delta.tv_usec = end.tv_usec - start.tv_usec;
        if (delta.tv_usec < 0) {
            delta.tv_sec -= 1;
            delta.tv_usec += 1000000;
        }
        printf(
            "%4d:%02d.%06"PRIdMAX"   %s\n",
            (int)delta.tv_sec/60,  (int)delta.tv_sec%60, (intmax_t)delta.tv_usec,
            label);

        if (munmap(aux, auxsz)==-1) {
            err(EXIT_FAILURE, "munmap");
        }
    }

};


void *benchmark::mem = NULL;
size_t benchmark::memsz = 0;
data_elem *benchmark::first = 0;
size_t benchmark::n = 0;


/*
 * Straightforward - sort element is a pointer to the actual data
 */
struct DefTraits
{
    struct sortel
    {
        data_elem *p;

        void init(data_elem &e)
        {
            p = &e;
        }

        bool operator < (const sortel &other) const
        {
            return memcmp(p->key, other.p->key, sizeof p->key)<0;
        }
    };
};


/*
 * Sort element agregates a sort element of some other type and augments
 * it with a prefix of the key for faster data comparisons
 */
template<int len, typename T=DefTraits>
struct PrefixTraits
{
    struct sortel
    {
        typename T::sortel nested;
        unsigned char prefix[len];

        void init(data_elem &e)
        {
            nested.init(e);
            memcpy(prefix, e.key, len);
        }

        bool operator < (const sortel &other) const
        {
            int s = memcmp(prefix, other.prefix, len);
            return (s ? s<0 : nested<other.nested);
        }
    };
};


/*
 * Since data was allocated in a contiguous chunk offsets of a smaller
 * range and consequently of a smaller size than a regular pointer could
 * be used
 */
template<typename offset_t = int32_t>
struct OffsetTraits
{
    struct sortel
    {
        offset_t offset;

        void init(data_elem &e)
        {
            offset = (offset_t)(&e - benchmark::first);
        }

        bool operator < (const sortel &other) const
        {
            return memcmp((benchmark::first+offset)->key, (benchmark::first+other.offset)->key, sizeof benchmark::first->key)<0;
        }
    };
};


/*
 * This is used to simulate a 'perfect' prefix, i.e. as if it was always
 * enough to compare just prefixes to determine data ordering; the
 * actual data is never accessed
 */
template<int len>
struct FakeTraits
{
    struct sortel
    {
        unsigned char dummy[len];
        void init(data_elem &e)
        {
        }

        bool operator < (const sortel &other) const
        {
            return false;
        }
    };
};



int main ()
{
    benchmark::init(1<<25, 60);
    printf(
        "Legend:\n"
        "\tptr - sorting array of pointers\n"
        "\toffset - sorting array of offsets (smaller elements)\n"
        "\tprefixX - array elements include X bytes prefix of a key\n"
        "\tperfect-prefixX - acting as if prefix comparison alone is enough\n");

    DefTraits d;
    benchmark::run("ptr", d);

    OffsetTraits<> off;
    benchmark::run("offset", d);

    PrefixTraits<8> p8;
    benchmark::run("prefix8", p8);

    PrefixTraits<8, FakeTraits<8> > fake_p8;
    benchmark::run("perfect-prefix8", fake_p8);

    PrefixTraits<4,OffsetTraits<> > offset_p4;
    benchmark::run("offset-prefix4", offset_p4);

    PrefixTraits<4,FakeTraits<4> > fake_offset_p4;
    benchmark::run("offset-perfect-prefix4", fake_offset_p4);

    PrefixTraits<12,OffsetTraits<> > offset_p12;
    benchmark::run("offset-prefix12", offset_p12);

    PrefixTraits<12,FakeTraits<4> > fake_offset_p12;
    benchmark::run("offset-perfect-prefix12", fake_offset_p12);

    return 0;
}
