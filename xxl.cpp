#include "util.hpp"

#include <cstddef>
#include <memory>
#include <deque>
#include <vector>

/*

struct test {
    unsigned char        key[64];
    uint64_t        flags;
    uint64_t        crc;
    uint64_t        size;
};

*/
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


file_id_t sort_some(parser<record_header> &input, const mem_chunk &available_mem)
{
    mem_chunk output_mem;
    mem_chunk membuf_mem;

    available_mem.split_at(8 * MiB, output_mem, membuf_mem);

    render_buf   membuf(membuf_mem);
    sort_element   *vb, *ve;

    vb = ve = reinterpret_cast<sort_element *>(membuf.get_free_mem().end());

    if (!input.is_header_valid() && !input.parse_next()) {
        return file_id_t();
    }

    do {
        size_t available_sz = membuf.get_free_mem().size();
        size_t reserved_sz = (ve - vb + 1)*(sizeof *vb);
        const record_header &hd = input.get_header();

        /*
         * Memory layout:
         *
         * DATA DATA DATA .... DATA -> FREE FREE FREE .... FREE <- P P P .... P
         */
        if (available_sz < alignof(hd) + sizeof(hd) + hd.body_size + reserved_sz) {
            break;
        } else {

            membuf.align(alignof(hd));
            sort_element::init(*(--vb), membuf.put(hd));

            mem_chunk buf = membuf.get_free_mem();
            input.read_body(buf);
            membuf.write(buf);
        }
    }
    while (input.parse_next());

    std::sort(vb, ve);

    file_id_t output_file_id = file_id::create_temporary("yndx-xxlsort");
    render_buf output(output_mem, output_file_id);
    for (sort_element *i = vb; i != ve; i++) {
        output.put(i->get_header());
        output.write(i->get_body());
    }
    output.flush();

    return output_file_id;
}


class merge_element
{
    public:
        merge_element(parser<record_header> &stream_)
        {
            stream = &stream_;
            key = stream->get_header().key;
        }
        bool operator < (const merge_element &other) const
        {
            /* in fact this is >=, since the operator is used by
             * std::make_/push_/pop_heap functions which put the largest
             * element on the top of the heap while we want the smallest */
            return memcmp(key, other.key, sizeof(record_header::key)) >= 0;
        }
        bool output_record_and_parse_next(render_buf &output)
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
        const uint8_t           *key;
        parser<record_header>   *stream;
};


void merge_all(const file_id_t &res, std::deque<file_id_t> &files, const mem_chunk &available_mem_)
{
    std::vector<std::unique_ptr<parser<record_header>>> input_streams;
    std::vector<merge_element> merger;

    while (!files.empty())
    {
        mem_chunk available_mem = available_mem_;
        mem_chunk output_buf_mem;
        available_mem.split_at(100 * MiB, output_buf_mem, available_mem);

        input_streams.clear();
        merger.clear();

        const size_t input_buf_size = 25 * MiB;
        while (available_mem.size() >= input_buf_size && !files.empty()) {

            mem_chunk input_buf_mem;
            available_mem.split_at(input_buf_size, input_buf_mem, available_mem);

            std::unique_ptr<parser<record_header>> p(
                new parser<record_header>(input_buf_mem, files.front()));
            files.pop_front();

            if (p->is_header_valid()) {
                merger.push_back(*p);
                input_streams.push_back(std::move(p));
            }
        }

        bool is_final = files.empty();
        auto output_file_id = (is_final ? res : file_id::create_temporary("yndx-xxlsort"));
        render_buf output(output_buf_mem, output_file_id);

        std::make_heap(merger.begin(), merger.end());
        while (!merger.empty()) {

            std::pop_heap(merger.begin(), merger.end());

            if (merger.back().output_record_and_parse_next(output)) {
                std::push_heap(merger.begin(), merger.end());
            } else {
                merger.pop_back();
            }
        }

        output.flush();
    };
}
