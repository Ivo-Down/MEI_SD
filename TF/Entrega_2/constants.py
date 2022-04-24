# -------------------- Constants --------------------

# Default
M_INIT = 'init'
M_INIT_OK = 'init_ok'

M_READ = 'read'
M_READ_OK = 'read_ok'

M_WRITE = 'write'
M_WRITE_OK = 'write_ok'

M_CAS = 'cas'
M_CAS_OK = 'cas_ok'

M_ERROR = 'error'


# Custom
RPC_APPEND_ENTRIES = 'rpc_append_entries'
RPC_APPEND_FALSE = 'rpc_append_false'
RPC_APPEND_OK = 'rpc_append_ok'

RPC_REQUEST_VOTE = 'rpc_request_vote'
RPC_REQUEST_VOTE_FALSE = 'rpc_request_vote_false'
RPC_REQUEST_VOTE_OK = 'rpc_request_vote_ok'


# Raft details
HEARTBEAT_INTERVAL = 1          # in seconds
ELECTION_TIMEOUT = 2            # in seconds
TIMEOUT_INTERVAL = 1            # in seconds
MIN_REPLICATION_INTERVAL = 0.05 # in seconds