#include "util.hpp"

#include <stdexcept>
#include <cerrno>
#include <cassert>
#include <cstdarg>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <err.h>
#include <limits.h>


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
    res.sz &= ~(n - 1);
    return res;
}


file_id_t file_id::create_with_path(const std::string &path)
{
    return std::make_shared<file_id>(path, /* auto-unlink: */ false);
}


file_id_t file_id::create_temporary(const std::string &name_template)
{
    std::string path;
    path.reserve(PATH_MAX);

    const char *tmp_dir;
    (tmp_dir = getenv("TMP"))
        || (tmp_dir = getenv("TEMP"))
        || (tmp_dir = getenv("TMPDIR"))
        || (tmp_dir = "/tmp");

    path.append(tmp_dir);
    path.append("/");
    if (name_template.empty()) {
        path.append("XXXXXX");
    } else {
        path.append(name_template);
        path.append("-XXXXXX");
    }

    /* sort of a bad hack but who cares anyway? */
    int fd = mkstemp(const_cast<char *>(path.c_str()));
    if (fd == -1) {
        std::string message = format_message_with_errno(
            errno, "Creating temporary file");
        throw std::runtime_error(message);
    }
    if (close(fd) == -1) {
        warn("Closing %s", path.c_str());
    }
    return std::make_shared<file_id>(path, /* auto_unlink: */ true);
}


file_id::~file_id()
{
    if (auto_unlink) {
        if (unlink(path.c_str()) == -1) {
            warn("Unlinking %s", path.c_str());
        }
    }
}


file_base::file_base(const file_id_t &id_, int mode)
    : fd(-1), id(id_), pos(0)
{
    if (id) {
        fd = open(get_file_path().c_str(), mode, S_IRUSR|S_IWUSR);
        if (fd == -1) {
            std::string message = format_message_with_errno(
                errno, "Error opening %s", get_file_path().c_str());
            throw std::runtime_error(message);
        }
    }
}


file_base::~file_base()
{
    if (fd != -1) {
        while (close(fd) == -1) {

            /* close() can fail with EINTR, not kidding */
            if (errno == EINTR) {
                continue;
            }

            warn("Closing %s", get_file_path().c_str());
            break;
        }
    }
}


const std::string &file_base::get_file_path() const
{
    if (id) {
        return id->get_path();
    }
    throw std::logic_error("NULL file");
}


int file_base::get_fd() const
{
    if (fd != -1) {
        return fd;
    }
    throw std::logic_error("NULL file");
}


bool file_base::is_seekable() const
{
    struct stat st;
    if (fstat(get_fd(), &st) == -1) {
        warn("fstat");
        return false;
    }
    return (st.st_mode & S_IFREG);
}


void file_base::set_file_pos(file_pos_t new_pos)
{
    if (pos == new_pos) {
        return;
    }
    pos = new_pos;
    if (lseek(get_fd(), pos, SEEK_SET)==-1) {
        std::string message = format_message_with_errno(
            errno, "Seeking in %s", get_file_path().c_str());
        throw std::runtime_error(message);
    }
}


input_file::input_file(const file_id_t &id)
    : file_base(id, O_RDONLY)
{
}


bool input_file::read(mem_chunk &data)
{
    uint8_t *p = data.begin(), *e = data.end();
    while (p < e) {
        ssize_t s = ::read(get_fd(), p, e - p);
        if (s == 0) {
            break;
        }
        if (s > 0) {
            p += s;
            pos += s;
        } else if (errno != EINTR) {
            std::string message = format_message_with_errno(
                errno, "Reading from %s", get_file_path().c_str());
            throw std::runtime_error(message);
        }
    }
    data = mem_chunk(data.begin(), p - data.begin());
    return (data.size() > 0);
}


output_file::output_file(const file_id_t &id)
    : file_base(id, O_WRONLY|O_CREAT|O_TRUNC)
{
}


void output_file::write(const mem_chunk &data)
{
    uint8_t *p = data.begin(), *e = data.end();
    while (p < e) {

        ssize_t s = ::write(get_fd(), p, e - p);
        if (s >= 0) {
            p += s;
            pos += s;
        } else if (errno != EINTR) {
            std::string message = format_message_with_errno(
                errno, "Writing to %s", get_file_path().c_str());
            throw std::runtime_error(message);
        }
    }
}


void output_file::flush()
{
    while (fsync(get_fd()) == -1) {

        if (errno == EINTR) {
            continue;
        }

        if (errno == EINVAL) {
            /* socket, pipe, etc */
            return;
        }

        std::string message = format_message_with_errno(
            errno, "Flushing %s", get_file_path().c_str());
        throw std::runtime_error(message);
    }
}


render_buf::render_buf(const mem_chunk &mem_, const file_id_t &id)
    : f(id), mem(mem_.aligned()), data(mem.sub_chunk(0, 0))
{
}


void render_buf::flush()
{
    f.write(data);
    /* to keep memory/file alignment in sync */
    data = data.sub_chunk(data.size(), -1);
    f.flush();
}


mem_chunk render_buf::get_free_mem()
{
    mem_chunk free_mem = mem.sub_chunk(data.end() - mem.begin(), -1);
    if (free_mem.empty()) {
        f.write(data);
        data = mem.sub_chunk(0, 0);
        free_mem = mem;
    }
    return free_mem;
}


void *render_buf::write(const mem_chunk &bytes_)
{
    void *origin;
    mem_chunk bytes = bytes_;
    while (!bytes.empty()) {
        mem_chunk free_mem = get_free_mem();
        origin = data.end(); /* a possibility for funny results here */
        mem_chunk put_portion;
        bytes.split_at(free_mem.size(), put_portion, bytes);
        data.append(put_portion);
    }
    return origin;
}


void render_buf::skip(size_t num_bytes)
{
    while (num_bytes > 0) {
        mem_chunk buf = get_free_mem().sub_chunk(0, num_bytes);
        buf.zero_memory();
        write(buf);
        num_bytes -= buf.size();
    }
}


void render_buf::align(size_t n)
{
    assert_alignment_valid(n);

    auto origin = reinterpret_cast<uintptr_t>(data.end());
    skip(((origin + n - 1) & ~(n - 1)) - origin);
}


parse_buf::parse_buf(const mem_chunk &mem_, const file_id_t &id)
    : f(id), mem(mem_.aligned())
{
}


bool parse_buf::read(mem_chunk &bytes_)
{
    mem_chunk bytes = bytes_.sub_chunk(0, 0);
    while (bytes.size() < bytes_.size()) {
        if (data.empty()) {
            /* to keep memory/file alignment in sync */
            data = mem.sub_chunk(
                static_cast<size_t>(f.get_file_pos() & (mem_chunk::ALIGNMENT_MAX - 1)), -1);
            if (!f.read(data)) {
                break;
            }
        }
        mem_chunk read_portion;
        data.split_at(bytes_.size() - bytes.size(), read_portion, data);
        bytes.append(read_portion);
    }
    bytes_ = bytes;
    return !bytes.empty();
}


void parse_buf::skip(size_t num_bytes)
{
    if (num_bytes <= data.size()) {
        data = data.sub_chunk(num_bytes, -1);
    } else {
        num_bytes -= data.size();
        data = mem_chunk();
        f.set_file_pos(f.get_file_pos() + num_bytes);
    }
}


void parse_buf::align(size_t n)
{
    assert_alignment_valid(n);

    auto origin = reinterpret_cast<uintptr_t>(data.end());
    skip(((origin + n - 1) & ~(n - 1)) - origin);
}


struct vasprintf_utility
{
    char *message;
    vasprintf_utility(const char *fmt, va_list ap)
    {
        if (vasprintf(&message, fmt, ap) == -1) {
            message = NULL;
        }
    }
    ~vasprintf_utility()
    {
        free(message);
    }
};


std::string format_message(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vasprintf_utility formatter(fmt, ap);
    va_end(ap);
    if (!formatter.message) {
        throw std::bad_alloc();
    }
    return formatter.message;
}


std::string format_message_with_errno(int error, const char *fmt, ...)
{
    char error_buf[128];
    va_list ap;
    va_start(ap, fmt);
    vasprintf_utility formatter(fmt, ap);
    va_end(ap);
    if (!formatter.message) {
        throw std::bad_alloc();
    }
    strerror_r(error, error_buf, sizeof error_buf);
    return format_message(
        "%s: %s", formatter.message, error_buf);
}

