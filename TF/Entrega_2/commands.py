from ms import *
import logging
from constants import *

logging.getLogger().setLevel(logging.DEBUG)

# Initializes the Node
# Returns the quorum size, node_ids, node_id
def handle_init(msg):
    node_id = msg['body']['node_id']
    node_ids = msg['body']['node_ids']
    logging.info('node %s initialized', node_id)
    
    replySimple(msg, type=M_INIT_OK)
    return node_ids, node_id


