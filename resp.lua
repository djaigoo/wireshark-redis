do
    -- 监听端口
    local listen_port = {
        6379,
    }

    local RESP = Proto('redis', 'Redis')

    local resp_types = {
        -- $
        [0x24] = 'bulk',
        -- *
        [0x2a] = 'multi-bulk',
        -- +
        [0x2b] = 'status',
        -- -
        [0x2d] = 'error',
        -- :
        [0x3a] = 'integer',
    }

    local data_type = ProtoField.char("redis.data.type", "data type", base.NONE, resp_types)
    local data_length = ProtoField.string("redis.data.len", "data length")
    local data_value = ProtoField.string("redis.data.val", "data value")

    RESP.fields = {
        data_type,
        data_length,
        data_value,
    }

    local CRLF = 2 -- constant length of \r\n

    local function matches(buffer, match_offset)
        local line = buffer(match_offset):string():match('[^\r\n]+')
        return line
    end

    local function join(buf, arr, s)
        local str = ""
        local i = 1
        while (i < #arr) do
            local k = arr[i]
            local v = arr[i + 1]
            if (k + v > buf:len()) then
                return str
            end
            str = str .. buf:raw(k, v) .. s
            i = i + 2
        end
        return str
    end

    local function merge(dst, src)
        for _, v in ipairs(src) do
            table.insert(dst, v)
        end
        return dst
    end

    local function setkv(arr, k, v)
        table.insert(arr, k)
        table.insert(arr, v)
    end

    local function handle_line(buf, pinfo, parent, offset, site)
        local line = matches(buf, offset)
        local length = line:len()
        local prefix, text = line:match('([-+:$*])(.+)')
        if not prefix or not text then
            pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
            return -1
        end
        local mtype = resp_types[prefix:byte()]
        if prefix == '*' then -- 数组
            local num = tonumber(text)
            local old_offset = offset
            local child = parent:add(RESP, buf(offset, length), 'redis operate: ')
            child:add(data_type, buf(offset, 1))
            child:add(data_length, buf(offset + 1, length - 1))
            offset = offset + length + CRLF

            local ssite = {}
            for ii = 1, num do
                offset = handle_line(buf, pinfo, child, offset, ssite)
                if offset == -1 then
                    pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
                    return -1
                end
            end
            child:append_text(join(buf, ssite, " "))
            merge(site, ssite)
            child:set_len(offset - old_offset)
        elseif prefix == '$' then -- 定长字符串
            local num = tonumber(text)
            if num == -1 then
                local child = parent:add(RESP, buf(offset, length + CRLF), mtype)
                offset = offset + length + CRLF
                child:add(data_value, '<null>')
            else
                if (buf:len() < offset + length + CRLF + num + CRLF) then
                    pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
                    return -1
                end
                local child = parent:add(RESP, buf(offset, length + CRLF + num + CRLF), mtype)
                child:add(data_type, buf(offset, 1))
                child:add(data_length, buf(offset + 1, length - 1))
                offset = offset + length + CRLF
                child:add(data_value, buf(offset, num))
                setkv(site, offset, num)
                offset = offset + num + CRLF
            end
        else -- integer, status or error
            if (buf:len() < offset + length + CRLF) then
                pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
                return -1
            end
            local child = parent:add(RESP, buf(offset, length + CRLF), mtype)
            child:add(data_type, buf(offset, 1))
            child:add(data_value, buf(offset + prefix:len(), length - prefix:len()))
            setkv(site, offset + prefix:len(), length - prefix:len())
            offset = offset + length + CRLF
        end
        return offset
    end

    local function resp_dissector(buf, pinfo, root)
        pinfo.cols.protocol = 'redis'
        local child = root:add(RESP, buf(0, buf:len()), 'Redis Reply')
        local offset = 0
        local site = {}
        while (offset < buf:len())
        do
            local ret = handle_line(buf, pinfo, child, offset, site)
            if (ret == -1) then
                return false
            end
            offset = ret
        end
        pinfo.cols.info = tostring(pinfo.src_port) .. " → " .. tostring(pinfo.dst_port) .. " " .. join(buf, site, " ")
        return true
    end

    local data_dis = Dissector.get("data")

    function RESP.dissector(buf, pinfo, root)
        if resp_dissector(buf, pinfo, root) then

        else
            data_dis:call(buf, pinfo, root)
        end
    end

    local dissectors = DissectorTable.get('tcp.port')
    for _, port in ipairs(listen_port) do
        dissectors:add(port, RESP)
    end
end
