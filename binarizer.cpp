/*
 * Converting sample data from textual to binary format.
 *
 *    KEYKEYKEYKEY  FLAGS  CRC  BODY_SIZE  BODY_SEED
 *
 * Test plan:
 * ----------
 *
 * 1. Generate sample data with a script, textual format;
 * 2. Convert to binary and run xxlsort;
 * 3. Derive reference data by sorting initial textual sample and
 *    converting it to binary.
 *
 */
#include "util.hpp"

#include <cinttypes>
#include <random>


struct record_header {
    uint8_t        key[64];
    uint64_t       flags;
    uint64_t       crc;
    file_size_t    body_size;
    uint8_t        body[1];
};


template <>
struct repr_traits<record_header>
{
    enum
    {
        ALIGNMENT = 1,
        SIZE = offsetof(record_header, body)
    };
};


int main()
{
    try {
        render_buf output(mem_chunk(malloc(40 * MiB), 40 * MiB), file_id::create_with_path("/dev/fd/1"));

        char line[1024];
        char key[1024];
        uint64_t flags, crc, body_size, body_seed;

        while (fgets(line, sizeof line, stdin)) {

            int st = sscanf(line,
                /* KEY FLAGS CRC BODY_SIZE BODY_SEED */
                "%s %"SCNd64" %"SCNd64" %"SCNd64" %"SCNd64,
                key,
                &flags,
                &crc,
                &body_size,
                &body_seed);

            if (st != 5 || body_size > 100 * MiB) {
                fprintf(stderr, "Line ignored\n");
                continue;
            }

            record_header hd;
            strncpy((char *)hd.key, key, 64);
            hd.flags = flags;
            hd.crc = crc;
            hd.body_size = body_size;

            output.put(hd);

            std::mt19937 rng(body_seed);
            std::uniform_int_distribution<uint8_t> random_byte;

            for (file_size_t i=0; i<body_size; i++) {
                output.put(random_byte(rng));
            }
        }
        output.flush();

        return EXIT_SUCCESS;
    }
    catch (const std::exception &e) {
        fprintf(stderr, "%s\n", e.what());
        return EXIT_FAILURE;
    }
}
