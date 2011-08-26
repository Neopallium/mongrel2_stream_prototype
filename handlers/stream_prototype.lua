--
-- Prototype XREQ/XREP stream interface for mongrel2
--
-- Author: Robert G. Jakabosky <bobby@sharedrealm.com>
--

local time = os.time

local zmq = require"zmq"
local z_SNDMORE = zmq.SNDMORE
local z_RCVMORE = zmq.RCVMORE
local zpoller = require"zmq.poller"

local vprint = function() end

-- check for verbose mode
for i=1,#arg do
	if arg[i] == '-V' then
		vprint = print
	end
end

local STREAM_BODY = true

local m2_ident = "9c9522ca-73af-4bac-a412-8802f71e87ca"

local config = {}
config.m2 = {
send_ident = "3164b0d4-1f64-4409-b940-225994f71aa3", -- this handler's identity.
pub_addr = "tcp://127.0.0.1:5558",
pull_addr = "tcp://127.0.0.1:5559",
}
config.stream = {
send_ident = m2_ident,
recv_ident = "",
sub_addr = "tcp://127.0.0.1:9996",
xrep_addr = "tcp://127.0.0.1:9997",
push_addr = "tcp://127.0.0.1:9998",
}

local async = require 'handlers.async'
local request = require"handlers.async.request"
local new_request = request.new

local tns = require"tnetstrings"
local t_parse = tns.parse

-- Create new mongrel2 async context
local mctx = assert(async.new(io_threads))

-- Creates a new async connection handler
local m2_conn = assert(mctx:new_connection{
handler_ident = config.m2.send_ident,
pull_addr = config.m2.pull_addr,
pub_addr = config.m2.pub_addr,
})

-- get 0mq context
local zctx = mctx.zctx

-- by-pass normal mongrel2 connection handling.
local m2 = {
pull = m2_conn.pull,
pub = m2_conn.pub,
}

-- prepare connections for backend handler that use new stream interface
local stream = {
sub = zctx:socket(zmq.SUB),
xrep = zctx:socket(zmq.XREP),
push = zctx:socket(zmq.PUSH),
}
assert(stream.push:setopt(zmq.IDENTITY, config.stream.send_ident))
assert(stream.push:bind(config.stream.push_addr))
assert(stream.xrep:setopt(zmq.IDENTITY, config.stream.send_ident))
assert(stream.xrep:bind(config.stream.xrep_addr))

assert(stream.sub:setopt(zmq.SUBSCRIBE, config.stream.recv_ident))
assert(stream.sub:bind(config.stream.sub_addr))

local function handle_error(action, err)
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

local function send_msg(sock, msg)
	-- send address
	local addr = msg.addr
	for i=1,#addr do
		vprint("send: addr:", addr[i])
		sock:send(addr[i], z_SNDMORE)
	end
	vprint("send: end addr:")
	sock:send("", z_SNDMORE)
	local len = #msg
	for i=1,len do
		vprint("send: msg:", msg[i])
		if i < len then
			sock:send(msg[i], z_SNDMORE)
		else
			sock:send(msg[i])
		end
	end
end

local function send_ack_msg(msg, result, data)
	local req = msg[1]
	msg[1] = "ACK_" .. req .. " " .. result
	msg[2] = data
	vprint("---------- send ACK response:")
	return send_msg(stream.xrep,msg)
end

-- command ack queue.
local acks = {}
local function push_ack(msg)
	acks[#acks + 1] = msg
end

local function flush_acks()
	for i=1,#acks do
		local msg = acks[i]
		acks[i] = nil
		send_ack_msg(msg, "FINISHED")
	end
end

-- list of client state
local clients = {}

local client_mt = {}
client_mt.__index = client_mt

function client_mt:close_body()
	local file = self.body
	if file then
		file:close()
		self.body = nil
	end
	local fname = self.body_file
	if fname then
		os.remove(fname)
		self.body_file = nil
	end
end

function client_mt:close()
	vprint("closing client state for id:", self.id)
	clients[self.id] = nil
	self:close_body()
end

function client_mt:remove()
	vprint("removing client state for id:", self.id)
	clients[self.id] = nil
	self:close_body()
end

local SEND_DATA = "%s %d:%s, %s"
function client_mt:write(msg)
	local data = msg[2]
	local id = tostring(self.id)
	m2.pub:send(SEND_DATA:format(m2_ident, #id, id, data))
	-- queue ack
	push_ack(msg)
end

function client_mt:read(msg, off)
	local len = msg[1]:match("(%d+)", off)
	local id = tostring(self.id)
	local results = " FINISHED"
	local body = self.body
	local data
	if body then
		data = body:read(tonumber(len))
		-- check for end of file.
		if data == nil then
			-- close body file.
			self:close_body()
			results = " CLOSED"
		end
	else
		results = " CLOSED"
	end
	-- send read response
	msg[1] = "ACK_READ " .. id .. results
	msg[2] = data
	send_msg(stream.xrep, msg)
end

local function get_client(req)
	local id = tonumber(req.conn_id)
	local client = clients[id]
	if not client then
		vprint("create client state for id:", id)
		-- create new client state.
		client = setmetatable({
			id = id,
			req = req,
		}, client_mt)
		clients[id] = client
	end
	return client
end

local function handle_request(msg)
	-- parse request
	local req, err = new_request(self, msg)
	if not req then return nil, err end

	-- get client state.
	local client = get_client(req)
	if req:is_disconnect() then
		client:close()
		return msg
	end
	-- check if stream request body is disabled.
	if not STREAM_BODY then return msg end

	-- handle request body
	local headers = req.headers
	if headers['x-mongrel2-upload-done'] then
		local expected = headers['x-mongrel2-upload-start'] or "BAD"
		local upload = headers['x-mongrel2-upload-done']
		vprint("UPLOAD DONE:", upload)
		if expected ~= upload then
			-- bad upload
			print("GOT THE WRONG TARGET FILE: ", expected, upload)
			return false
		end
		-- open temp. file
		local file = assert(io.open(upload, "r"))
		client.body = file
		client.body_file = upload
		-- remove old temp. file upload headers
		headers['x-mongrel2-upload-start'] = nil
		headers['x-mongrel2-upload-done'] = nil
		headers['x-mongrel2-stream-upload'] = 1
		-- re-encode request
		return req:encode()
	elseif headers['x-mongrel2-upload-start'] then
		vprint("UPLOAD starting, don't forward request yet.")
		vprint("Will read file from ", headers['x-mongrel2-upload-start'])
		return false
	end

	return msg
end

local function handle_response(msg)
	-- parse response
	local uuid, ids, off = msg:match("([^ ]*) %d+:([0-9 ]+), ()")
	--local body_len = #msg - off
	-- handle each id
	for id in ids:gmatch("%d+") do
		local client = clients[tonumber(id)]
		if client then
			-- Remove client state for connections that don't use stream interface.
			client:remove()
		else
			print("Missing client state for id:", id)
		end
	end
end

local function handle_stream_request(msg)
	-- parse stream request
	local req = msg[1]
	local cmd, id, off = req:match("([^ ]+) (%d+)()")
	local client = clients[tonumber(id)]
	vprint("stream request client:", cmd, client)
	if not client then
		-- send closed response.
		send_ack_msg(msg, "CLOSED")
		return
	end

	-- handle request
	if cmd == 'WRITE' then
		client:write(msg, off)
	elseif cmd == 'READ' then
		client:read(msg, off)
	elseif cmd == 'CLOSE' then
		-- This command is for cleaning up client state (i.e. temp files).
		client:close()
		-- send ack for close command
		send_ack_msg(msg, "CLOSED")
	end
end

-- use a new 0mq poller object
local poller = assert(zpoller(8))

-- proxy request/responses from frontend mongrel2 to backend stream handler.
poller:add(m2.pull, zmq.POLLIN, function()
	local raw, err = m2.pull:recv()
	vprint("request from mongrel2: [" .. raw .. ']')
	if not raw then return handle_error("m2.pull:recv()", err) end
	-- handle the request
	local raw, err = handle_request(raw)
	if raw == nil then return handle_error("m2.pull:recv()", err) end
	-- forward raw request from mongrel2 to backend stream handler.
	if raw ~= false then
		assert(stream.push:send(raw))
	end
end)

poller:add(stream.sub, zmq.POLLIN, function()
	local raw, err = stream.sub:recv()
	if not raw then return handle_error("stream.sub:recv()", err) end
	vprint("response from handler: [" .. raw .. ']')
	-- forward raw response from backend stream handler to mongrel2
	assert(m2.pub:send(raw))
	-- handle response
	handle_response(raw)
end)

poller:add(stream.xrep, zmq.POLLIN, function()
	vprint("------------ got stream request:")
	local msg = recv_msg(stream.xrep)
	handle_stream_request(msg)
end)

-- start poller's event loop
local last_flush = time()
while poller:poll(100 * 1000) do
	local ts = time()
	-- delay sending acks by about 1 second, just to simulate slow I/O
	if (ts - last_flush) >= 1 then
		flush_acks()
		last_flush = ts
	end
end

-- We never get here but clean up anyhow
stream.push:close()
stream.sub:close()
stream.xrep:close()
mctx:term()

