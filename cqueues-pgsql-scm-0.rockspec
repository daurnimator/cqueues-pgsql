package = "cqueues-pgsql"
version = "scm-0"
source = {
	url = "git://github.com/daurnimator/cqueues-pgsql";
}
description = {
	summary = "A wrapper around luapgsql that uses cqueues";
	detailed = [[
		When used within a cqueues event loop your postgres operations will be non-blocking!
		API is identical to luapgsql, please refer to documentation there.
	]];
	homepage = "https://github.com/daurnimator/cqueues-pgsql";
	license = "MIT/X11";
}
dependencies = {
	"lua >= 5.1";
	"cqueues";
	"luapgsql >= 1.6.1";
}
build = {
	type = "builtin";
	modules = {
		['cqueues_pgsql'] = "cqueues_pgsql.lua";
	};
}
