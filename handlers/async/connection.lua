--
-- Async. connection object.
--
-- Author: Robert G. Jakabosky <bobby@sharedrealm.com>
--

local setmetatable = setmetatable
local tostring = tostring
local tonumber = tonumber
local tconcat = table.concat
local pairs = pairs
local print = print

local vprint = function() end

-- check for verbose mode
for i=1,#arg do
	if arg[i] == '-V' then
		vprint = print
	end
end

local chunked = require"handlers.chunked"
local chunked = chunked.new

local zmq = require"zmq"
local z_SNDMORE = zmq.SNDMORE
local z_RCVMORE = zmq.RCVMORE
local zpoller = require"zmq.poller"

local json = require"json"
local json_encode = json.encode.getEncoder{
	initialObject = true,
}

local function http_encode(buf, off, body, code, status, headers, http_version)
	-- append HTTP Response-Line
	off = off + 1
	buf[off] = http_version or "HTTP/1.1"
	off = off + 1
	buf[off] = " "
	off = off + 1
	buf[off] = code or 200
	off = off + 1
	buf[off] = " "
	off = off + 1
	buf[off] = status or 'OK'
	off = off + 1
	buf[off] = "\r\n"

	local stream_body
	local body_len
	if body then
		local btype = type(body)
		if btype == 'string' then
			-- add "Content-Length" header
			headers = headers or {}
			headers["content-length"] = #body
		elseif btype == 'function' then
			-- check for "Content-Length" header
			if headers and headers["content-length"] then
				-- no chunked encoding needed
				stream_body = body
			else
				-- must chunk encode body
				stream_body = chunked(body)
				-- add "Transfer-Encoding" header
				headers = headers or {}
				headers["Transfer-Encoding"] = "chunked"
			end
			-- don't append body
			body = nil
		end
	end

	-- append HTTP headers
	if headers then
		for k,v in pairs(headers) do
			off = off + 1
			buf[off] = k
			off = off + 1
			buf[off] = ": "
			off = off + 1
			buf[off] = v
			off = off + 1
			buf[off] = "\r\n"
		end
	end
	-- end HTTP Response-Headers
	off = off + 1
	buf[off] = "\r\n"

	-- append Response body
	if body then
		off = off + 1
		buf[off] = body
	end

	return buf, off, stream_body
end

local request = require"handlers.async.request"
local new_request = request.new

local mt = {}
mt.__index = mt

local function prepare_resp(buf, uuid, conn_id)
	local off = 1
	-- UUID
	buf[off] = uuid
	off = off + 1
	buf[off] = " "
	-- encode connection ids
	if type(conn_id) == 'table' then
		conn_id = tconcat(conn_id, ' ')
	else
		conn_id = tostring(conn_id)
	end
	off = off + 1
	buf[off] = #conn_id
	off = off + 1
	buf[off] = ":"
	off = off + 1
	buf[off] = conn_id
	off = off + 1
	buf[off] = ", "

	return buf, off
end

local function prepare_resp_multi(buf, uuid, ids)
	-- concat list of connection ids.
	return prepare_resp(buf, uuid, tconcat(ids, ' '))
end

local function concat_buf(buf, off)
	local data = tconcat(buf, '', 1, off)
	-- clear temp. buffer
	for i=off,1,-1 do
		buf[i] = nil
	end
	return data
end

local function send(self, buf, off)
	local msg = tconcat(buf, '', 1, off)
	local rc, err = self.pub:send(msg)
	-- clear temp. buffer
	for i=off,1,-1 do
		buf[i] = nil
	end
	return rc, err
end

local function send_stream_request(self, uuid, req, data)
	local xreq = self.xreq
	assert(xreq, "Missing streaming socket.")
	local rc, err
	-- send XREQ routing address
	xreq:send(uuid, z_SNDMORE)
	xreq:send("", z_SNDMORE)
	-- send request part
	xreq:send(req, data and z_SNDMORE or 0)
	if data then
		-- send data chunk
		xreq:send(data)
	end
	return true
end

local function reg_callback(self, cmd, conn_id, cb)
	local cbs = self.cbs[cmd]
	assert(cbs, "Missing streaming support.")
	cbs[conn_id] = cb
end

local function send_notify(self, cmd, uuid, conn_id, data, cb, params)
	local req = cmd .. " " .. conn_id
	if params then
		req = req .. " " .. params
	end
	-- register callback for when stream request is finished.
	reg_callback(self, cmd, conn_id, cb)
	-- send stream request
	return send_stream_request(self, uuid, req, data)
end

function mt:send(uuid, conn_id, msg)
	local buf, off = prepare_resp(self.tmp_buf, uuid, conn_id)
	off = off + 1
	buf[off] = msg
	return send(self, buf, off)
end

function mt:send_notify(uuid, conn_id, msg, cb)
	return send_notify(self, "WRITE", uuid, conn_id, msg, cb)
end

function mt:read_notify(uuid, conn_id, len, cb)
	return send_notify(self, "READ", uuid, conn_id, nil, cb, len or 8192)
end

function mt:send_json(uuid, conn_id, data)
	local buf, off = prepare_resp(self.tmp_buf, uuid, conn_id)
	off = off + 1
	buf[off] = json_encode(data)
	return send(self, buf, off)
end

function mt:send_http(uuid, conn_id, body, code, status, headers, http_version)
	if type(body) == 'function' then
		conn_id = tonumber(conn_id)
		-- prepare HTTP response
		local buf, off, stream = http_encode(self.tmp_buf, 0, body, code, status, headers, http_version)
		local data = concat_buf(buf, off)
		local function stream_cb(self, conn_id, result, msg)
			if result ~= "FINISHED" then
				-- connection closed or some other error.
				return
			end
			-- get next body chunk to send
			local data = stream()
			if data then
				-- send chunk
				return send_notify(self, "WRITE", uuid, conn_id, data, stream_cb)
			end
			-- data == nil, finished
		end
		return send_notify(self, "WRITE", uuid, conn_id, data, stream_cb)
	end
	local buf, off = prepare_resp(self.tmp_buf, uuid, conn_id)
	buf, off = http_encode(buf, off, body, code, status, headers, http_version)
	return send(self, buf, off)
end

function mt:deliver(uuid, ids, msg)
	local buf, off = prepare_resp(self.tmp_buf, uuid, ids)
	off = off + 1
	buf[off] = msg
	return send(self, buf, off)
end

function mt:deliver_json(uuid, ids, data)
	local buf, off = prepare_resp(self.tmp_buf, uuid, ids)
	off = off + 1
	buf[off] = json_encode(data)
	return send(self, buf, off)
end

function mt:deliver_http(uuid, ids, body, code, status, headers, http_version)
	assert(not body or type(body) == 'string', "Can't stream to multiple connections.")
	local buf, off = prepare_resp(self.tmp_buf, uuid, ids)
	buf, off = http_encode(buf, off, body, code, status, headers, http_version)
	return send(self, buf, off)
end

function mt:close(req)
	-- check if the caller is just closing a request object.
	if req then return req:close() end

	-- check if connection is already closed
	if self.is_closed then return end
	self.is_closed = true

	local poller = self.ctx.poller
	-- close sockets
	if self.pull then
		self.pull:close()
		poller:remove(self.pull)
		self.pull = nil
	end
	if self.pub then
		self.pub:close()
		poller:remove(self.pub)
		self.pub = nil
	end
	if self.xreq then
		self.xreq:close()
		poller:remove(self.xreq)
		self.xreq = nil
	end

	-- remove connection from main context.
	self.ctx:close_connection(self)
end

local function handle_error(self, action, err)
	local cb = self.on_error
	if cb then
		return cb(self, action, err)
	end
	return print("ERROR:", action, err)
end

local function recv_msg(sock)
	local addr = {}
	local msg = {addr = addr}
	local parse_addr = true
	vprint("--------------------------------")
	while true do
		local data = sock:recv()
		vprint("from stream.xrep: [" .. data .. ']')
		if parse_addr then
			if #data > 0 then
				addr[#addr + 1] = data
			else
				-- finished parsing return address.
				parse_addr = false
				vprint("=====")
			end
		else
			msg[#msg + 1] = data
		end
		if (sock:getopt(z_RCVMORE) == 0) then
			break
		end
	end
	return msg
end

local function handle_response(self)
	local msg = recv_msg(self.xreq)
	-- parse command response header
	local req = msg[1]
	local cmd, conn_id, result = req:match("ACK_([^ ]+) (%d+) (.+)")
	conn_id = tonumber(conn_id)
	local cbs = self.cbs[cmd]
	if not cbs then
		-- invalid command.
		print("Response for unknown command:", cmd)
		return
	end
	local cb = cbs[conn_id]
	vprint("stream response:", cmd, conn_id, type(conn_id), result, cb)
	if cb then
		cbs[conn_id] = nil
		cb(self, conn_id, result, msg)
	end
end

module(...)

function new(ctx, self)
	local rc, err
	local pull, pub, xreq
	local zctx = ctx.zctx
	local poller = ctx.poller

	self.ctx = ctx

	-- create temp. buffer for response encoding.
	self.tmp_buf = {}

	-- connect PULL socket to mongrel2
	pull, err = zctx:socket(zmq.PULL)
	if not pull then return nil, err end
	self.pull = pull
	rc, err = pull:connect(self.pull_addr)
	if not rc then return nil, err end

	-- connect PUB socket to mongrel2
	pub, err = zctx:socket(zmq.PUB)
	if not pub then return nil, err end
	self.pub = pub
	rc, err = pub:setopt(zmq.IDENTITY, self.handler_ident)
	if not rc then return nil, err end
	rc, err = pub:connect(self.pub_addr)
	if not rc then return nil, err end

	-- poll for new requests from mongrel2
	poller:add(pull, zmq.POLLIN, function()
		local req, err = pull:recv()
		if not req then return handle_error(self, "pull:recv()", err) end
		req, err = new_request(self, req)
		if not req then return handle_error(self, "parse request", err) end
		return self:on_request(req)
	end)

	-- connect XREQ socket to mongrel2, if we are given an address
	if self.xreq_addr then
		xreq, err = zctx:socket(zmq.XREQ)
		if not xreq then return nil, err end
		self.xreq = xreq
		rc, err = xreq:setopt(zmq.IDENTITY, self.handler_ident)
		if not rc then return nil, err end
		rc, err = xreq:connect(self.xreq_addr)
		if not rc then return nil, err end
		-- create callback list
		self.cbs = { WRITE = {}, READ = {}}

		-- poll for responses from mongrel2 (responses to our stream requests)
		poller:add(xreq, zmq.POLLIN, function()
			vprint("------------------- got stream response:")
			handle_response(self)
		end)
	end

	return setmetatable(self, mt)
end


