import random
import time
from uuid import uuid4

main_host = 'http://127.0.0.1:'

node_current_state = {
    "role": "follower",
    "data": dict(),
    "log": [],
    "repl": [[f"{main_host}{port}", 1] for port in range(5030, 5033)],
    "master": None,
    "node_id": str(uuid4()),
    "term": 0,
    "votes": 0,
    "hb": time.time(),
    "election_timeout": random.uniform(2, 5),
    "port": None,
}
