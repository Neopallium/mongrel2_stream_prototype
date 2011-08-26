--
-- Async. mongrel2 handler
--
-- Author: Robert G. Jakabosky <bobby@sharedrealm.com>
--

local assert = assert
local setmetatable = setmetatable
local tremove = table.remove

local zmq = require"zmq"
local zpoller = require"zmq.poller"

local conn = require"handlers.async.connection"
local new_conn = conn.new

local mt = {}
mt.__index = mt

function mt:new_connection(...)
	local cons = self.cons
	local con, err = new_conn(self, ...)
	if con then
		cons[#cons + 1] = con
	end
	return con, err
end

function mt:close_connection(con)
	local cons = self.cons
	for i=1,#cons do
		if cons[i] == con then
			tremove(cons, i)
			con:close()
			return
		end
	end
	con:close()
end

local function term(self)
	local cons = self.cons
	self.cons = {}
	for i=1,#cons do
		local con = cons[i]
		con:close()
	end
	if self.zctx then
		self.zctx:term()
		self.zctx = nil
	end
end

function mt:term()
	self.exit_loop = true
end

function mt:poll(timeout)
	self.poller:poll(timeout)
	return not self.exit_loop
end

function mt:start()
	self.exit_loop = false
	repeat
		self:poll(-1)
	until self.exit_loop
	self:term()
end

module(...)

function new(io_theads)
	local zctx, err = zmq.init(io_threads or 1)
	if not zctx then return nil, err end

	local poller = assert(zpoller(8))
	return setmetatable({
		zctx = zctx,
		poller = poller,
		cons = {},
	}, mt)
end

