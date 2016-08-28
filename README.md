A wrapper around [luapgsql](https://github.com/arcapos/luapgsql) that uses [cqueues](http://25thandclement.com/~william/projects/cqueues.html)

When used within a cqueues event loop your postgres operations will be non-blocking!

This library tries to match the luapgsql API exactly.

Compatible with Lua 5.1 (and LuaJIT), 5.2 and 5.3.
