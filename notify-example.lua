local cqueues = require "cqueues"
local pgsql = require "cqueues_pgsql"

local conn = pgsql.connectdb ""
if conn:status() ~= pgsql.CONNECTION_OK then
	error(conn:errorMessage(), nil)
end
assert(conn:exec("LISTEN somechannel"):status() == pgsql.PGRES_COMMAND_OK)

local loop = cqueues.new()

loop:wrap(function()
	while true do
		if cqueues.poll({pollfd = conn:socket(); events = "r"}) then
			conn:consumeInput()
			local n = conn:notifies()
			if n then
				print("NOTIFIED", n:relname(), n:pid(), n:extra())
			end
		end
	end
end)
loop:wrap(function()
	while true do
		if conn:exec("NOTIFY somechannel, 'hi!'"):status() ~= pgsql.PGRES_COMMAND_OK then
			error(conn:errorMessage(), nil)
		end
		print("SENT NOTIFY")
		cqueues.sleep(2)
	end
end)

assert(loop:loop())
