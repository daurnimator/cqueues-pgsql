local cqueues = require "cqueues"
local pgsql = require "cqueues_pgsql"

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


-- Works like normal in a standard lua context:
do
	local conn = pgsql.connectdb ""
	if conn:status() ~= pgsql.CONNECTION_OK then
		error(conn:errorMessage(), nil)
	end

	local res = conn:exec("SELECT * FROM sometable")
	if not res then error(conn:errorMessage(), nil) end
	pr(res)
end

-- But when inside a cqueues coroutine, it's non blocking!
local loop = cqueues.new()
loop:wrap(function()
	local conn = pgsql.connectdb ""
	if conn:status() ~= pgsql.CONNECTION_OK then
		error(conn:errorMessage(), nil)
	end

	do
		local res = conn:exec("SELECT * FROM sometable")
		if not res then error(conn:errorMessage(), nil) end
		pr(res)
	end

	do
		local prepared = conn:prepare("sel", [[SELECT $1 AS "heh" FROM sometable]], "asd")
		if not prepared or prepared:status() ~= pgsql.PGRES_COMMAND_OK then
			error(prepared:errorMessage(), nil)
		end
	end

	do
		local res = conn:execPrepared("sel", "id")
		if not res then error(conn:errorMessage(), nil) end
		pr(res)
	end
end)
assert(loop:loop())

