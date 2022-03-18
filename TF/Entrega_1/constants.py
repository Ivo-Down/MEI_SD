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

QR_CAS_COMP_SET = 'qr_cas_comp_set'     # compares and sets the value
QR_CAS_SET = 'qr_cas_set'         # sets the value when the version more recent
QR_CAS_OK = 'qr_cas_ok'         # cas was done successfuly on the qr node

QR_READ_WRITE = 'qr_read_write'
QR_READ_WRITE_OK = 'qr_read_write_ok'

QR_ERROR = 'qr_error'
QR_ERROR_WRITE = 'qr_error_write'

# CODES

CODE_UNAVAILABLE = 11
CODE_KEY_MISSING = 20