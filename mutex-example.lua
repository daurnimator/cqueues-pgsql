local cqueues = require "cqueues"
local condition = require "cqueues.condition"
local pgsql = require "cqueues-pgsql"

-- Simple function that prints out a result object
local function pr(res)
	print("STATUS: ", res:status())
	for j=1, res:nfields() do
		print(res:fname(j))
	end
	for i=1, res:ntuples() do	
		for j=1, res:nfields() do
			print(res:getvalue(i, j))
		end
	end
end


local conn = pgsql.connectdb()
if conn:status() ~= pgsql.CONNECTION_OK then
	error(conn:errorMessage(), nil)
end
assert(conn:exec("LISTEN somechannel"):status() == pgsql.PGRES_COMMAND_OK)

local loop = cqueues.new()
local mutex do
	local inuse = false
	local cond = condition.new(true)
	mutex = {}
	function mutex:lock(timeout)
		if inuse then
			inuse = cond:wait(timeout)
		else
			inuse = true
		end
		return inuse
	end
	function mutex:unlock()
		inuse = false
		cond:signal(1)
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
