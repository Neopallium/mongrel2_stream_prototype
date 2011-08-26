--
-- Async. handler request object
--
-- Author: Robert G. Jakabosky <bobby@sharedrealm.com>
--

local setmetatable = setmetatable
local pcall = pcall
local type = type
local assert = assert
local print = print
local tonumber = tonumber

local zmq = require"zmq"
local zpoller = require"zmq.poller"

local tns = require"tnetstrings"
local t_parse = tns.parse
local t_encode = tns.dump

local json = require"json"
local json_decode = json.decode.getDecoder{
}
local json_encode = json.encode.getEncoder{
}
local function j_parse(data)
	local status, results = pcall(json_decode, data)
	if not status then
		return nil, results
	end
	return results
end

local mt = {}
mt.__index = mt

function mt:close()
	self.is_closed = true
end

function mt:is_disconnect()
	local json = self.json
	return json and json.type == 'disconnect'
end

function mt:should_close()
	local headers = self.headers
	return (headers.connection == 'close') or (headers.VERSION == 'HTTP/1.0')
end

function mt:close()
	return self.con:send(self.sender, self.conn_id, "")
end

function mt:stream_reply()
	self.is_streaming = true
end

function mt:reply(msg)
	return self.con:send(self.sender, self.conn_id, msg)
end

function mt:reply_json(data)
	return self.con:send_json(self.sender, self.conn_id, data)
end

function mt:reply_http(body, code, status, headers)
	return self.con:send_http(self.sender, self.conn_id,
		body, code, status, headers,
		self.headers.VERSION)
end

function mt:read(len, cb)
	return self.con:read_notify(self.sender, self.conn_id, len, cb)
end

function mt:encode()
	local msg = self.sender .. ' ' .. self.conn_id .. ' ' .. self.path .. ' '
	-- encode headers
	if self.hformat == 'tnet' then
		msg = msg .. t_encode(self.headers)
	else
		local j_headers = json_encode(self.headers)
		msg = msg .. ("%d:%s,"):format(#j_headers, j_headers)
	end
	-- encode body
	local body = self.body
	if type(body) == 'string' then
		msg = msg .. ("%d:%s,"):format(#body, body)
	elseif body then
		body = json_encode(body)
		msg = msg .. ("%d:%s,"):format(#body, body)
	else
		msg = msg .. "0:,"
	end
	return msg
end

module(...)

function new(con, msg)
	local err
	-- parse: "UUID ID PATH "
	local sender, conn_id, path, off = msg:match("([^ ]*) ([^ ]*) ([^ ]*) ()")

	-- parse headers
	local headers, off = t_parse(msg, nil, off)
	local hformat = 'tnet'
	-- check if headers are in JSON format
	local htype = type(headers)
	if htype == 'string' then
		headers, err = j_parse(headers)
		if not headers then return nil, err end
		hformat = 'json'
	else
		assert(htype == 'table', "Unknown header format.")
	end
	local method = headers.METHOD

	-- parse body
	local body, err = t_parse(msg, ',', off)
	local json
	if not body then return nil, err end
	-- check if body is JSON
	if method == 'JSON' then
		json, err = j_parse(body)
		if not json then return nil, err end
	end

	return setmetatable({
		con = con,
		sender = sender,
		conn_id = tonumber(conn_id),
		path = path,
		headers = headers,
		hformat = hformat,
		method = method,
		body = body,
		json = json,
	}, mt)
end

