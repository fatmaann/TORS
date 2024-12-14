import random
import time
from uuid import uuid4

main_host = 'http://127.0.0.1:'


class RaftNode:
    def __init__(self):
        self.node_id: str = str(uuid4())
        self.port: str = ''
        self.role: str = "follower"
        self.term: int = 0

        self.data: dict = dict()
        self.log = []
        self.network: list[list[str | int]] = [[f"{main_host}{port}", 1] for port in range(5030, 5033)]
        self.raft_master: str = ''
        self.election_votes: int = 0

        self.heartbeat = time.time()
        self.re_election = random.uniform(2, 5)

    def heartbeat_json(self):
        return {"term": self.term, "master_id": self.raft_master, "data": self.data, "log_data": self.log}

    def candidate_json(self):
        return {"candidate_id": self.node_id, "term": self.term}

    def processing_election(self, update_term):
        self.role = "follower"
        self.election_votes = 0
        self.term = update_term
        self.heartbeat = time.time()

    def becoming_master(self):
        self.role = "master"
        self.election_votes = 0
        self.raft_master = f"{main_host}{self.port}"

    def still_follower(self):
        self.role = "follower"
        self.election_votes = 0

    def processing_hb(self, update_master: str, update_term: int, update_data: dict, update_log: list):
        self.role = "follower"
        self.raft_master = update_master
        self.term = update_term
        self.heartbeat = time.time()
        self.data = update_data
        self.log = update_log
