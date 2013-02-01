/*
 * This is going to be C++11 since we are going to use shared_ptr but
 * requiring boost would be lame :)
 *
 * Targeting POSIX.
 *
 */
#pragma once

#include <memory>
#include <string>
#include <cassert>


std::string format_message(const char *fmt, ...);
std::string format_message_with_errno(int error, const char *fmt, ...);


enum
{
    KiB = 1024,
    MiB = 1024 * KiB,
    GiB = 1024 * MiB
};


typedef uint64_t file_pos_t, file_size_t;


class file_id;
typedef std::shared_ptr<file_id> file_id_t;


/*
 * Something that identifies a file on disk (as opposed to an open
 * file).  File is auto-unlinked in dtor (optional).
 */
class file_id
{
    public:
        static file_id_t create_with_path(const std::string &path);
        static file_id_t create_temporary();

        const std::string &get_path() const { return path; }
        void set_auto_unlink(bool enable) { auto_unlink = enable; }

        ~file_id();
        file_id(const std::string &path_, bool auto_unlink_)
            : path(path_), auto_unlink(auto_unlink_)
        {
        }

    private:
        std::string path;
        bool auto_unlink;
};


/*
 * Glorified raw memory chunk
 */
struct mem_chunk
{
    uint8_t *p;
    size_t size;

    enum {
        ALIGNMENT_MAX = 64 * KiB
    };

    mem_chunk(void *p_ = 0, size_t size_ = 0)
        : p(static_cast<uint8_t *>(p_)), size(size_) { ; }
    bool empty() const { return size == 0; }
    uint8_t *begin() const { return p; }
    uint8_t *end() const { return p + size; }
    inline mem_chunk aligned(size_t n) const;
    mem_chunk sub_chunk(size_t offset, size_t size_) const
    {
        size_t origin = std::min(offset, size);
        return mem_chunk(p + origin, std::min(size_, size - origin));
    }
    void split_at(size_t pos, mem_chunk &left, mem_chunk &right)
    {
        uint8_t *p = this->p;
        size_t size = this->size;
        pos = std::min(pos, size);
        left = mem_chunk(p, pos);
        right = mem_chunk(p + pos, size - pos);
    }
    void append(const mem_chunk &other)
    {
        if (end() != other.begin()) {
            memcpy(end(), other.p, other.size);
        }
        size += other.size;
    }
    void zero_memory() const { memset(p, 0, size); }
};


inline void assert_alignment_valid(size_t n)
{
    assert(n > 0);
    assert((n & (n - 1)) == 0);
    assert(n <= mem_chunk::ALIGNMENT_MAX);
}


mem_chunk mem_chunk::aligned(size_t n) const
{
    assert_alignment_valid(n);

    auto origin = reinterpret_cast<uintptr_t>(p);
    auto res = sub_chunk(((origin + n - 1) & ~(n - 1)) - origin, -1);
    res.size &= ~(n - 1);
    return res;
}


/*
 * The base class for input_/output_file classes.  Our IO classes throw
 * exceptions on IO error.  File doesn't need to be seekable though an
 * attempt to set_file_pos() will trigger error if it is not.
 *
 * Actually there doesn't need to be a file at all - null_ptr is allowed
 * for the file id (obviously all operations with such a "file" will
 * fail).
 */
class file_base
{
    public:
        file_base(const file_id_t &id, int mode);
        ~file_base();
        const file_id_t &get_file_id() const { return id; }
        const std::string &get_file_path() const;
        file_pos_t get_file_pos() const { return pos; }
        void set_file_pos(file_pos_t new_pos);
        bool is_seekable() const;
    protected:
        int get_fd() const;
    private:
        int        fd;
        file_id_t  id;
    protected:
        file_pos_t pos;
};


class input_file: public file_base
{
    public:
        input_file(const file_id_t &id);
        /* Reads data in memory and updates size. Returns false iff
         * resulting size==0  (EOF) */
        bool read(mem_chunk &data);
};


class output_file: public file_base
{
    public:
        output_file(const file_id_t &id);
        void write(const mem_chunk &data);
        /* Explicit flushing helps to avoid IO errors from close() in
         * file_base dtor */
        void flush();
};


/*
 * Controls some aspect of T representation produced by
 * render_buf::put<T> and consumed by parse_buf::read<T>.
 */
template <typename T>
struct repr_traits
{
    enum
    {
        ALIGNMENT = 1,
        SIZE = sizeof(T)
    };
};


/* Producing output data  (memory buffer + optional output file) */
class render_buf
{
    public:
        render_buf(const mem_chunk &mem, const file_id_t &output_file_id = file_id_t());
        void flush();
        mem_chunk get_free_mem();
        void *put(const mem_chunk &data);
        void skip(size_t num_bytes);
        void align(size_t n);
        file_pos_t get_file_pos() const { return f.get_file_pos() + data.size; }

        template <typename T>
        T *put(const T &v)
        {
            if (repr_traits<T>::ALIGNMENT != 1) {
                align(repr_traits<T>::ALIGNMENT);
            }
            mem_chunk c(const_cast<T*>(&v), repr_traits<T>::SIZE);
            return static_cast<T *>(put(c));
        }

    private:
        output_file     f;
        mem_chunk       mem;
        mem_chunk       data;
};


/* Consuming input data  (memory buffer + input file) */
class parse_buf
{
    public:
        parse_buf(const mem_chunk &mem, const file_id_t &input_file_id);
        bool read(mem_chunk &bytes);
        void skip(size_t num_bytes);
        void align(size_t n);
        file_pos_t get_file_pos() const { return f.get_file_pos() - data.size; }

        template <typename T>
        bool read(T &v)
        {
            if (repr_traits<T>::ALIGNMENT != 1) {
                align(repr_traits<T>::ALIGNMENT);
            }
            mem_chunk c(&v, repr_traits<T>::SIZE);
            return read(c) && c.size == repr_traits<T>::SIZE;
        }

    private:
        input_file      f;
        mem_chunk       mem;
        mem_chunk       data;
};


/* Parsing a stream of records consisting of the fixed size header and
 * the variable length body. Throws exception if encountered malformed data */
template <typename header_t, typename external_header_t = header_t>
class parser
{
    public:
        parser(
            const mem_chunk &mem,
            const file_id_t &input_file_id
        )
            : buf(mem, input_file_id), hd_valid(false), body_bytes_left(0)
        {
        }
        /*
         * Skip over the current record if any and parse another one.
         * Returns false on EOF
         */
        bool parse_next()
        {
            buf.skip(body_bytes_left);
            external_header_t dummy;
            return (hd_valid = parse_header(buf, dummy, hd, body_bytes_left));
        }
        bool is_header_valid() const { return hd_valid; }
        /*
         * Get current record's header (precondition: preceeding call to
         * parse_next() returned true)
         */
        header_t &get_header() const { return hd; }
        /*
         * Read current record's body. Updates mem size. Returns false
         * when the body is over.
         */
        bool read_body(mem_chunk &body_chunk)
        {
            size_t chunk_size = body_chunk.size = std::min(body_chunk.size, body_bytes_left);
            if (chunk_size == 0) {
                return false;
            }
            buf.read(body_chunk, chunk_size);
            if (body_chunk.size != chunk_size) {
                throw std::runtime_error("Data corrupt");
            }
            body_bytes_left -= chunk_size;
            return true;
        }
    private:
        parse_buf  buf;
        header_t   hd;
        bool       hd_valid;
        file_size_t     body_bytes_left;
};
