
local md5 = require"md5"

local async = require 'handlers.async'

local sender_id = '558c92aa-1644-4e24-a524-39baad0f8e78'
local pub_addr = 'tcp://127.0.0.1:9996'
local xreq_addr = 'tcp://127.0.0.1:9997'
local pull_addr = 'tcp://127.0.0.1:9998'
local io_threads = 1

local ltn12 = require"ltn12"
local src_string = ltn12.source.string
local src_cat = ltn12.source.cat

local socket = require"socket"
local sleep = socket.sleep

local response_string = [[
<html>
<head><title>Stream test</title></head>
<body>
<pre>
SENDER: %s
IDENT: %s
PATH: %s
HEADERS: %s
REQUEST BODY: %s
RESPONSE STREAM:
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

local counter = 0
local function fake_stream()
	counter = counter + 1
	if counter < 4 then
		return ("counter: %d<br>\n"):format(counter)
	end
	counter = 0
	return nil
end

local function send_response(req, headers)
	-- echo request.
	local response = response_string:format(
		req.sender, req.conn_id, req.path, dump(headers), req.body
	)

	assert(req:reply_http(src_cat(src_string(response),fake_stream)))
end

local function on_request(self, req)
	local headers = req.headers

	print("dump request headers:", dump(headers))
	if req:is_disconnect() then
		print("DISCONNECT")
	elseif headers['x-mongrel2-stream-upload'] then
		local block_size = 16 * 1024
		local function download_file(self, conn_id, result, msg)
			local data = msg[2]
			print("Stream file from browser:", conn_id, result, data and #data or 'END')
			if data then
				-- more data.
				req:read(block_size, download_file)
			else
				send_response(req, req.headers)
			end
		end
		print("Start transfer file from browser!")
		req:read(block_size, download_file)
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
			assert(req:reply_http("UPLOAD GOOD: " .. md5.sumhexa(body)))
			os.remove(upload)
		end
	elseif headers['x-mongrel2-upload-start'] then
		print("UPLOAD starting, don't reply yet.")
		print("Will read file from ", headers['x-mongrel2-upload-start'])
	else
		send_response(req, headers)
	end
end


-- Create new mongrel2 async context
local ctx = assert(async.new(io_threads))

-- Creates a new async connection handler
local conn = assert(ctx:new_connection{
handler_ident = sender_id,
pull_addr = pull_addr,
pub_addr = pub_addr,
xreq_addr = xreq_addr,
on_request = on_request,
})

ctx:start()

