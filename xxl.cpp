bool parse_header(parse_buf &buf, test &external_hd, test &hd, file_size_t &body_size)
{
    if (!buf.read(external_hd)) {
        return false;
    }
    hd = external_hd; /* same format */
    body_size = hd.size;
    return true;
}


struct sort_item
{
    test *p;
    static void init(sort_item &i, test *p) { i->p = p; }
    bool operator < (const sort_item &other) const
    {
        return memcmp(p->key, other.p->key, sizeof p->key) < 0;
    }
    mem_chunk get_chunk() const
    {
        return mem_chunk();
    }
};


file_id_t sort_some(parser<test> &input, const mem_chunk &available_mem)
{
    mem_chunk output_mem;
    mem_chunk membuf_mem;

    available_mem.split_at(8 * MiB, output_mem, membuf_mem);

    render_buf   membuf(membuf_mem);
    sort_item   *vb, *ve;

    vb = ve = static_cast<sort_item *>(membuf.get_free_mem().end());

    if (!input.is_header_valid() && !input.parse_next()) {
        return file_id_t();
    }

    do {
        size_t available_sz = membuf.get_free_mem().size;
        size_t reserved_sz = (ve - vb + 1)*(sizeof *vb);
        test &hd = input.get_header();

        /*
         * Memory layout:
         *
         * DATA DATA DATA .... DATA -> FREE FREE FREE .... FREE <- P P P .... P
         */
        if (available_sz < alignof(hd) + sizeof(hd) + hd.size + reserved_sz) {
            break;
        } else {

            membuf.align(alignof(hd));
            sort_item::init(*(--vb), membuf.put(hd));

            mem_chunk buf = membuf.get_free_mem();
            input.read_body(buf);
            membuf.put(buf);
        }
    }
    while (input.parse_next());

    std::sort(vb, ve);

    file_id_t output_file_id = file_id::create_temporary();
    render_buf output(output_mem, output_file_id);
    for (sort_item *i = vb; i != ve; i++) {
        output.put(i->get_chunk());
    }
    output.flush();

    return output_file_id;
}
