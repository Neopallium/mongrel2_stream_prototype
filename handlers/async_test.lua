local async = require 'handlers.async'

local sender_id = '558c92aa-1644-4e24-a524-39baad0f8e78'
local sub_addr = 'tcp://127.0.0.1:5557'
local pub_addr = 'tcp://127.0.0.1:5556'
local io_threads = 1

local response_string = [[
<pre>
SENDER: %s
IDENT: %s
PATH: %s
HEADERS: %s
BODY: %s
</pre>
]]

local assert = assert
local format = string.format

local function dump(tab)
	local out = ''
	for k, v in pairs(tab) do
		out = format('%s\n-- [%s]: %s)', out, k, v)
	end
	return out
end

local utils = require"utils"
local function on_request(self, req)
	local response = response_string:format(
		req.sender, req.conn_id, req.path, dump(req.headers), req.body
	)

	assert(req:reply_http(response))
end


-- Create new mongrel2 async context
local ctx = assert(async.new(io_threads))

-- Creates a new async connection handler
local conn = assert(ctx:new_connection{
handler_ident = sender_id,
pull_addr = sub_addr,
pub_addr = pub_addr,
on_request = on_request,
})

ctx:start()

