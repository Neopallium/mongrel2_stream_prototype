--
-- Mongrel2 handler proxy for debugging.
--
-- Author: Robert G. Jakabosky <bobby@sharedrealm.com>
--
local zmq = require"zmq"
local zpoller = require"zmq.poller"

local m2_ident = "9c9522ca-73af-4bac-a412-8802f71e87ca"

local config = {}
config.m2 = {
send_ident = "3164b0d4-1f64-4409-b940-225994f71aa3", -- this handler's identity.
pub_addr = "tcp://127.0.0.1:5558",
pull_addr = "tcp://127.0.0.1:5559",
}
config.backend = {
send_ident = m2_ident,
recv_ident = "",
sub_addr = "tcp://127.0.0.1:9996",
push_addr = "tcp://127.0.0.1:9998",
}

local async = require 'handlers.async'

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

-- prepare connections for backend handler
local backend = {
sub = zctx:socket(zmq.SUB),
push = zctx:socket(zmq.PUSH),
}
assert(backend.push:setopt(zmq.IDENTITY, config.backend.send_ident))
assert(backend.push:bind(config.backend.push_addr))

assert(backend.sub:setopt(zmq.SUBSCRIBE, config.backend.recv_ident))
assert(backend.sub:bind(config.backend.sub_addr))

local client_mt = {}
client_mt.__index = client_mt

function client_mt:close()
end

-- list of client state
local clients = {}
local function get_client(req)
	local id = req.conn_id
	local client = clients[id]
	if not client then
		-- create new client state.
		client = setmetatable({ id = id, req = req }, client_mt)
		clients[id] = client
	end
	return client
end

-- proxy request/responses from frontend mongrel2 to backend handler
function m2:on_request(req)
	local client = get_client(req)
	if req:is_disconnect() then
		client:close()
	end
end

-- use a new 0mq poller object
local poller = assert(zpoller(8))

poller:add(m2.pull, zmq.POLLIN, function()
	local req = assert(m2.pull:recv())
	print("request from mongrel2: [" .. req .. ']')
	-- forward request from mongrel2 to backend stream handler.
	assert(backend.push:send(req))
end)

poller:add(backend.sub, zmq.POLLIN, function()
	local resp = assert(backend.sub:recv())
	print("response from handler: [" .. resp .. ']')
	-- forward response from backend stream handler to mongrel2
	assert(m2.pub:send(resp))
end)

-- start poller's event loop
--[[
while poller:poll(100 * 1000) do
end
--]]
poller:start()

-- We never get here but clean up anyhow
backend.push:close()
backend.sub:close()
mctx:term()

