local pgsql = require "pgsql"
local cqueues = require "cqueues"

local methods = {}
local mt = {
	__name = "cqueues-pgsql connection";
}
-- Delegate to underlying pgsql object
function mt.__index(t,k)
	local v = methods[k]
	if v ~= nil then return v end
	if k == "conn" then return nil end -- Don't want to accidently recurse
	local f = t.conn[k]
	-- If f is a function; need to wrap it so it gets the correct 'self'
	if type(f) == "function" then
		return function(s, ...)
			if s == t then
				s = s.conn
			end
			return f(s, ...)
		end
	else
		return f
	end
end

function methods:finish()
	local ok, fd = pcall(self.conn.socket, self.conn)
	if ok then
		cqueues.cancel(fd)
	end
	return self.conn:finish()
end

function mt:__gc()
	self:finish()
end

--- Override synchronous methods to yield via cqueues
function methods:connectPoll()
	while true do
		local polling = self.conn:connectPoll()
		if polling == pgsql.PGRES_POLLING_READING then
			cqueues.poll {
				pollfd = self.conn:socket();
				events = "r";
			}
		elseif polling == pgsql.PGRES_POLLING_WRITING then
			cqueues.poll {
				pollfd = self.conn:socket();
				events = "w";
			}
		else
			return polling
		end
	end
end
function methods:resetPoll()
	while true do
		local polling = self.conn:resetPoll()
		if polling == pgsql.PGRES_POLLING_READING then
			cqueues.poll {
				pollfd = self.conn:socket();
				events = "r";
			}
		elseif polling == pgsql.PGRES_POLLING_WRITING then
			cqueues.poll {
				pollfd = self.conn:socket();
				events = "w";
			}
		else
			return polling
		end
	end
end
function methods:reset()
	cqueues.cancel(self.conn:socket())

	if self.conn:resetStart() == 0 then
		return
	end
	while true do
		local status = self.conn:status()
		if status == pgsql.CONNECTION_OK then
			break
		elseif status == pgsql.CONNECTION_BAD then
			break
		end
		if self.conn:resetPoll() ~= pgsql.PGRES_POLLING_OK then
			break
		end
	end
end
function methods:flush()
	local t
	while true do
		local r = self.conn:flush()
		if r == 1 then
			if not t then
				t = {
					pollfd = self.conn:socket();
					events = "w";
				}
			end
			cqueues.poll(t)
		else
			return r
		end
	end
end
function methods:sendQuery(...)
	if self.conn:sendQuery(...) == 0 then
		return 0
	end
	if self:flush() == 0 then
		return 1
	else -- returned -1
		return 0
	end
end
function methods:sendQueryParams(...)
	if self.conn:sendQueryParams(...) == 0 then
		return 0
	end
	if self:flush() == 0 then
		return 1
	else -- returned -1
		return 0
	end
end
function methods:sendPrepare(...)
	if self.conn:sendPrepare(...) == 0 then
		return 0
	end
	if self:flush() == 0 then
		return 1
	else -- returned -1
		return 0
	end
end
function methods:sendQueryPrepared(...)
	if self.conn:sendQueryPrepared(...) == 0 then
		return 0
	end
	if self:flush() == 0 then
		return 1
	else -- returned -1
		return 0
	end
end
function methods:sendDescribePrepared(...)
	if self.conn:sendDescribePrepared(...) == 0 then
		return 0
	end
	if self:flush() == 0 then
		return 1
	else -- returned -1
		return 0
	end
end
function methods:sendDescribePortal(...)
	if self.conn:sendDescribePortal(...) == 0 then
		return 0
	end
	if self:flush() == 0 then
		return 1
	else -- returned -1
		return 0
	end
end
function methods:getResult()
	local t
	while self.conn:isBusy() do
		if not t then
			t = {
				pollfd = self.conn:socket();
				events = "r";
			}
		end
		cqueues.poll(t)
		if not self.conn:consumeInput() then
			-- error
			return nil
		end
	end
	return self.conn:getResult()
end
function methods:exec(...)
	if self:sendQuery(...) == 0 then
		return nil
	end
	-- return the last result
	local res
	while true do
		local tmp = self:getResult()
		if tmp == nil then
			return res
		else
			res = tmp
		end
	end
end
function methods:execParams(...)
	if self:sendQueryParams(...) == 0 then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end
function methods:prepare(...)
	if self:sendPrepare(...) == 0 then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end
function methods:execPrepared(...)
	if self:sendQueryPrepared(...) == 0 then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end
function methods:describePrepared(...)
	if self:sendDescribePrepared(...) == 0 then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end
function methods:describePortal(...)
	if self:sendDescribePortal(...) == 0 then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end

local function wrap(conn)
	conn:setnonblocking(true) -- Don't care if it fails
	return setmetatable({
			conn = conn;
		}, mt)
end
if _VERSION == "Lua 5.1" then
	-- Lua 5.1 does not respect __gc on tables
	-- However it does have newproxy.
	local old_wrap = wrap
	wrap = function(...)
		local self = old_wrap(...)
		local gc_hook = newproxy(true)
		getmetatable(gc_hook).__gc = function()
			self:finish()
		end
		self.gc_hook = gc_hook
		return self
	end
end

local function connectStart(...)
	return wrap(pgsql.connectStart(...))
end

local function connectdb(...)
	local conn = connectStart(...)
	while true do
		local status = conn:status()
		if status == pgsql.CONNECTION_OK then
			break
		elseif status == pgsql.CONNECTION_BAD then
			break
		end
		if conn:connectPoll() ~= pgsql.PGRES_POLLING_OK then
			break
		end
	end
	return conn
end

-- Get exports ready
local M = {
	connectStart = connectStart;
	connectdb = connectdb;
	libVersion = pgsql.libVersion;
	ping = pgsql.ping;
	unescapeBytea = pgsql.unescapeBytea;
	encryptPassword = pgsql.encryptPassword;
	initOpenSSL = pgsql.initOpenSSL;
}

-- Copy in constants
for k, v in pairs(pgsql) do
	if k == k:upper() and type(v) == "number" then
		M[k] = v
	end
end

return M
