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
QR_LOCK = 'qr_lock'             # -> pede ao nodo o seu lock
QR_LOCK_OK = 'qr_lock_ok'
QR_LOCK_FAIL = 'qr_lock_fail'   # -> lock já atribuido
QR_UNLOCK = 'qr_unlock'         # -> pede ao nodo para dar unlock

QR_READ = 'qr_read'
QR_READ_OK = 'qr_read_ok'       # -> read request response
QR_READ_FAIL = 'qr_read_fail'

QR_WRITE = 'qr_write'
QR_WRITE_OK = 'qr_write_ok'
QR_WRITE_FAIL = 'qr_write_fail'

QR_CAS = 'qr_cas'
QR_CAS_READ = 'qr_cas_read'
QR_CAS_READ_OK = 'qr_cas_read_ok'
QR_CAS_READ_FAIL = 'qr_cas_read_fail'
QR_CAS_WRITE = 'qr_cas_write'
QR_CAS_WRITE_OK = 'qr_cas_write_ok'
QR_CAS_WRITE_FAIL = 'qr_cas_write_fail'

# CODES
CODE_UNAVAILABLE = 11
CODE_KEY_MISSING = 20
CODE_PRE_COND_FAIL = 20