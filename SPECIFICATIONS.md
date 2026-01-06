# Python client for the FileEngine gRPC service

Migrate the adapter interface in file_engine_cpp/python/ as a stand-alone project.

The new implementation of the adapter must use the gRPC service implemented in
../file_engine_core/core/

Take special care to maintain compatability with the ManagedFiles class. But the new
constructor accepts optional parameters for the hostname and port of the gRPC server,
and optionally a tenant name. The constructor must accept the username, list of roles,
and list of claims.