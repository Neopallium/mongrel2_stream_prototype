
local md5 = require"md5"

local mongrel2 = require 'mongrel2'

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

-- Create new mongrel2 context
-- This is basically just a wrapper around the zeromq context so we can
-- properly terminate it, and set a number of threads to use.
local ctx = assert(mongrel2.new(io_threads))

-- Creates a new connection object using the mongrel2 context
local conn = assert(ctx:new_connection(sender_id, sub_addr, pub_addr))

local assert = assert
local format = string.format

local function dump(tab)
	local out = ''
	for k, v in pairs(tab) do
		out = format('%s\n-- [%s]: %s)', out, k, v)
	end
	return out
end

local function send_response(req)
	local response = format(response_string, req.sender, req.conn_id, req.path, dump(req.headers), req.body)

	assert(conn:reply_http(req, response))
end

while true do
	local req = assert(conn:recv())
	local headers = req.headers

	print("dump request headers:", dump(headers))
	if req:is_disconnect() then
		print("DISCONNECT")
	elseif headers['x-mongrel2-upload-done'] then
		local expected = headers['x-mongrel2-upload-start'] or "BAD"
		local upload = headers['x-mongrel2-upload-done']
		print("UPLOAD DONE:", upload)
		if expected ~= upload then
			-- bad upload
			print("GOT THE WRONG TARGET FILE: ", expected, upload)
		else
			local file = assert(io.open(upload, "r"))
			local body = assert(file:read("*a"))
			file:close()
			print("UPLOAD DONE: BODY IS " .. #body .. " long, content length is:",
				headers['content-length'])
			assert(conn:reply_http(req, "UPLOAD GOOD: " .. md5.sumhexa(body)))
			os.remove(upload)
		end
	elseif headers['x-mongrel2-upload-start'] then
		print("UPLOAD starting, don't reply yet.")
		print("Will read file from ", headers['x-mongrel2-upload-start'])
	else
		-- echo request.
		send_response(req)
	end
end

assert(ctx:term())

