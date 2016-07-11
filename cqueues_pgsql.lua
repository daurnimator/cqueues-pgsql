local pgsql = require "pgsql"
local cqueues = require "cqueues"

local methods = {}
local mt = {
	__name = "cqueues_pgsql connection";
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

	if not self.conn:resetStart() then
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
	local r, w
	while true do
		local res = self.conn:flush()
		if res ~= false then
			return res
		end
		if not r then
			r = {
				pollfd = self.conn:socket();
				events = "r";
			}
			w = {
				pollfd = self.conn:socket();
				events = "w";
			}
		end
		if cqueues.poll(r, w) == r then
			if not self.conn:consumeInput() then
				return nil
			end
		end
	end
end

function methods:sendQuery(...)
	if not self.conn:sendQuery(...) then
		return false
	end
	return self:flush() ~= nil
end

function methods:sendQueryParams(...)
	if not self.conn:sendQueryParams(...) then
		return false
	end
	return self:flush() ~= nil
end

function methods:sendPrepare(...)
	if not self.conn:sendPrepare(...) then
		return false
	end
	return self:flush() ~= nil
end

function methods:sendQueryPrepared(...)
	if not self.conn:sendQueryPrepared(...) then
		return false
	end
	return self:flush() ~= nil
end

function methods:sendDescribePrepared(...)
	if not self.conn:sendDescribePrepared(...) then
		return false
	end
	return self:flush() ~= nil
end

function methods:sendDescribePortal(...)
	if not self.conn:sendDescribePortal(...) then
		return false
	end
	return self:flush() ~= nil
end

function methods:getResult()
	if self.conn:isBusy() then
		-- Flush before consuming in case there is a pending outgoing data
		-- If we don't call it, consumeInput will anyway
		-- but then if a socket buffer is full we'd fall into a busy-loop
		if self:flush() == nil then
			return nil
		end

		local t = {
			pollfd = self.conn:socket();
			events = "r";
		}
		while true do
			if not self.conn:consumeInput() then
				-- error
				return nil
			end
			if not self.conn:isBusy() then
				break
			end
			cqueues.poll(t)
		end
	end
	return self.conn:getResult()
end

function methods:exec(...)
	if not self:sendQuery(...) then
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
	if not self:sendQueryParams(...) then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end

function methods:prepare(...)
	if not self:sendPrepare(...) then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end

function methods:execPrepared(...)
	if not self:sendQueryPrepared(...) then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end

function methods:describePrepared(...)
	if not self:sendDescribePrepared(...) then
		return nil
	end
	-- Can only have one result
	local res = self:getResult()
	-- Have to read until nil
	assert(self:getResult() == nil)
	return res
end

function methods:describePortal(...)
	if not self:sendDescribePortal(...) then
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
if _VERSION == "Lua 5.1" then -- luacheck: push std lua51
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
end -- luacheck: pop

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
