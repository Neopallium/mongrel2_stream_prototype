Mongrel2 prototype stream interface
====================================

Prototype a stream interface between mongrel2 and backend handlers.

The basic idea of the stream interface is to give backend handler flow-control over
sending/recving large amounts of data to/from the client connection in mongrel2.


Start test
---------------------------

Run mongrel2 with test config:

	cd m2
	m2sh start -host localhost --db config/config.sqlite


Install Lua dependencies:

	luarocks install lua-zmq
	wget "https://github.com/jsimmons/tnetstrings.lua/raw/master/rockspecs/tnetstrings-scm-0.rockspec"
	luarocks install tnetstrings-scm-0.rockspec
	luarocks install luasocket
	luarocks install luajson
	luarocks install md5


Start stream interface gateway handler:

	lua handlers/stream_prototype.lua 


Start stream based handler:

	lua handlers/chunked_stream_test.lua


Start old style test handler:

	lua handlers/direct_test.lua


Note: You can use the "-V" option to enable verbose output from the lua handlers.

