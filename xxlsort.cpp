#include "util.hpp"

#include <sys/mman.h>

#include <cstdlib>
#include <cstddef>
#include <memory>
#include <deque>
#include <vector>
#include <stdexcept>
#include <cstdio>
#include <cerrno>


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


/* Invoked by parser<record_header>::parse_next() */
bool parse_header(parse_buf &buf, record_header &external_hd, record_header &hd, file_size_t &body_size)
{
    if (!buf.get(external_hd)) {
        return false;
    }
    if (external_hd.body_size > 100 * MiB) {
        throw std::runtime_error("Malformed data");
    }
    hd = external_hd; /* same format */
    body_size = hd.body_size;
    return true;
}


class sort_element
{
    public:
        static void init(sort_element &i, record_header *p) { i.p = p; }
        bool operator < (const sort_element &other) const
        {
            return memcmp(p->key, other.p->key, sizeof p->key) < 0;
        }
        const record_header &get_header() const { return *p; }
        mem_chunk get_body() const
        {
            return mem_chunk(p->body, p->body_size);
        }
    private:
        record_header *p;
};


void split_and_sort(
    const mem_chunk &available_mem_,
    const file_id_t &src_file,
    const file_id_t &dest_file,
    std::deque<file_id_t> &transient_files)
{
    mem_chunk input_mem;
    mem_chunk available_mem;
    available_mem_.split_at(4 * MiB, input_mem, available_mem);

    parser<record_header> input(input_mem, src_file);

    int segment_no = 0;

    do {
        mem_chunk output_mem;
        mem_chunk membuf_mem;

        available_mem.split_at(25 * MiB, output_mem, membuf_mem);

        render_buf   membuf(membuf_mem);
        sort_element   *vb, *ve;

        vb = ve = reinterpret_cast<sort_element *>(membuf.get_free_mem().end());

        /*
         * Memory layout:
         *
         * DATA DATA DATA .... DATA -> FREE FREE FREE .... FREE <- P P P .... P
         */
        while (input.is_header_valid()) {
            size_t available_sz = membuf.get_free_mem().size();
            size_t reserved_sz = (ve - vb + 1)*(sizeof *vb);
            const record_header &hd = input.get_header();

            if (available_sz < alignof(hd) + sizeof(hd) + hd.body_size + reserved_sz) {
                break;
            }

            membuf.align(alignof(hd));
            sort_element::init(*(--vb), membuf.put(hd));

            mem_chunk buf = membuf.get_free_mem();
            input.read_body(buf);
            membuf.write(buf);

            input.parse_next();
        }
        std::sort(vb, ve);

        bool is_final = (segment_no==0 && !input.is_header_valid());
        file_id_t output_file_id;

        if (is_final) {
            output_file_id = dest_file;
        } else {
            output_file_id = file_id::create_temporary("yndx-xxlsort");
            transient_files.push_back(output_file_id);
        }

        render_buf output(output_mem, output_file_id);
        for (sort_element *i = vb; i != ve; i++) {
            output.put(i->get_header());
            output.write(i->get_body());
        }
        output.flush();
        segment_no ++;
    }
    while (input.is_header_valid());
}


class merge_element
{
    public:
        merge_element(parser<record_header> &stream_)
        {
            stream = &stream_;
        }
        bool operator < (const merge_element &other) const
        {
            /* in fact this is >=, since the operator is used by
             * std::make_/push_/pop_heap functions which put the largest
             * element on the top of the heap while we want the smallest */
            return memcmp(
                stream->get_header().key,
                other.stream->get_header().key, sizeof(record_header::key)) >= 0;
        }
        bool write_record_and_parse_next(render_buf &output)
        {
            output.put(stream->get_header());
            while (1) {
                mem_chunk buf = output.get_free_mem();
                if (!stream->read_body(buf)) {
                    break;
                }
                output.write(buf);
            }
            return stream->parse_next();
        }
    private:
        parser<record_header>   *stream;
};


void merge_sorted(
    const mem_chunk &available_mem_,
    const file_id_t &dest_file,
    std::deque<file_id_t> &transient_files)
{
    std::vector<std::unique_ptr<parser<record_header>>> input_streams;
    std::vector<merge_element> merger;

    while (!transient_files.empty())
    {
        mem_chunk available_mem = available_mem_;
        mem_chunk output_buf_mem;
        available_mem.split_at(40 * MiB, output_buf_mem, available_mem);

        input_streams.clear();
        merger.clear();

        const size_t input_buf_size = 25 * MiB;
        while (available_mem.size() >= input_buf_size && !transient_files.empty()) {

            mem_chunk input_buf_mem;
            available_mem.split_at(input_buf_size, input_buf_mem, available_mem);

            std::unique_ptr<parser<record_header>> p(
                new parser<record_header>(input_buf_mem, transient_files.front()));
            transient_files.pop_front();

            if (p->is_header_valid()) {
                merger.push_back(*p);
                input_streams.push_back(std::move(p));
            }
        }

        bool is_final = transient_files.empty();
        file_id_t output_file_id;

        if (is_final) {
            output_file_id = dest_file;
        } else {
            output_file_id = file_id::create_temporary("yndx-xxlsort");
            transient_files.push_back(output_file_id);
        }

        render_buf output(output_buf_mem, output_file_id);

        std::make_heap(merger.begin(), merger.end());
        while (!merger.empty()) {

            std::pop_heap(merger.begin(), merger.end());

            if (merger.back().write_record_and_parse_next(output)) {
                std::push_heap(merger.begin(), merger.end());
            } else {
                merger.pop_back();
            }
        }
        output.flush();
    }
}


size_t get_available_mem_size()
{
    const char *p = getenv("AVAILABLE_MEM");
    if (!p) {
        return 8 * GiB;
    } else {
        char *endp;
        double v = strtod(p, &endp);
        if (endp == p || (*endp && !strchr("kKmMgG", *endp)) || v < 0.0) {
            throw std::runtime_error(
                format_message("Invalid settings in env: AVAILABLE_MEM=%s", p));
        }
        switch (*endp) {
        default:
            return v;
        case 'k': case 'K':
            return v * KiB;
        case 'm': case 'M':
            return v * MiB;
        case 'g': case 'G':
            return v * GiB;
        }
    }
}


int main(int argc, char ** argv)
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s <input> <output>\n", argv[0]);
        return EXIT_FAILURE;
    }

    try {
        size_t size = get_available_mem_size();
        void *p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);
        if (p==MAP_FAILED) {
            throw std::runtime_error(
                format_message_with_errno(
                    errno,
                    "Allocating %zu bytes of memory", size));
        }

        mem_chunk available_mem = mem_chunk(p, size).aligned();

        file_id_t src_file = file_id::create_with_path(argv[1]);
        file_id_t dest_file = file_id::create_with_path(argv[2]);

        dest_file->set_auto_unlink(true);

        std::deque<file_id_t> transient_files;
        split_and_sort(available_mem, src_file, dest_file, transient_files);
        merge_sorted(available_mem, dest_file, transient_files);

        dest_file->set_auto_unlink(false);

        return EXIT_SUCCESS;
    }
    catch (const std::logic_error &e) {
        fprintf(stderr, "%s: Internal error: %s\n", argv[0], e.what());
        return EXIT_FAILURE;
    }
    catch (const std::exception &e) {
        fprintf(stderr, "%s: %s\n", argv[0], e.what());
        return EXIT_FAILURE;
    }
}
