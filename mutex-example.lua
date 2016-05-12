local cqueues = require "cqueues"
local condition = require "cqueues.condition"
local pgsql = require "cqueues-pgsql"

local conn = pgsql.connectdb()
if conn:status() ~= pgsql.CONNECTION_OK then
	error(conn:errorMessage(), nil)
end
assert(conn:exec("LISTEN somechannel"):status() == pgsql.PGRES_COMMAND_OK)

local loop = cqueues.new()
local mutex do
	mutex = {
		cond = condition.new(true);
		inuse = false;
	}
	function mutex:lock(timeout)
		if self.inuse then
			self.inuse = self.cond:wait(timeout)
		else
			self.inuse = true
		end
		return self.inuse
	end
	function mutex:unlock()
		self.inuse = false
		self.cond:signal(1)
	end
end

loop:wrap(function()
	while true do
		assert(mutex:lock())
		print("NUMBER 1 PRE")
		local res = conn:exec("SELECT * FROM sometable")
		if res == nil or res:status() ~= pgsql.PGRES_TUPLES_OK then
			local err = conn:errorMessage()
			mutex:unlock()
			error(err)
		end
		print("NUMBER 1 POST")
		mutex:unlock()
		cqueues.sleep(1)
	end
end)
loop:wrap(function()
	while true do
		assert(mutex:lock())
		print("NUMBER 2 PRE")
		local res = conn:exec("SELECT * FROM sometable")
		if res == nil or res:status() ~= pgsql.PGRES_TUPLES_OK then
			local err = conn:errorMessage()
			mutex:unlock()
			error(err)
		end
		print("NUMBER 2 POST")
		mutex:unlock()
		cqueues.sleep(2)
	end
end)

assert(loop:loop())
