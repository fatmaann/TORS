import threading
from typing import List


class SECMap:
    def __init__(self):
        self.clock = None
        self.node_id = None
        self.repls_amount = None
        self.node_state = {}  # {key: {"val": value, "timestamp": clock, "req_id": req_id}}
        self.lock = threading.Lock()

    def node_conf(self, node_id: int, repls_amount: int):
        self.node_id = node_id
        self.repls_amount = repls_amount
        self.clock = [0] * repls_amount

    def update_vector_clock(self):
        self.clock[self.node_id] += 1

    def perform_operation(self, key, entry):
        if self.cmp_vec_clock(entry):
            if entry["val"]:
                self.node_state[key] = entry
                self.clock = entry["timestamp"]
            else:
                if key in self.node_state:
                    del self.node_state[key]
                self.clock = entry["timestamp"]

    def cmp_vec_clock(self, entry):
        bool_new = False
        for i in range(self.repls_amount):
            if entry["timestamp"][i] > self.clock[i]:
                bool_new = True
            elif entry["timestamp"][i] < self.clock[i]:
                return False

        if not bool_new:
            return entry.get("req_id") > self.node_id
        return bool_new

    def add(self, key, value):
        with self.lock:
            self.update_vector_clock()
            entry = {
                "val": value,
                "timestamp": self.clock.copy(),
                "req_id": self.node_id,
            }
            self.node_state[key] = entry
            return key, entry

    def delete(self, key):
        with self.lock:
            self.update_vector_clock()
            if key in self.node_state:
                del self.node_state[key]
            entry = {"val": None, "timestamp": self.clock.copy(), "req_id": self.node_id}
            return key, entry

    def merge(self, incoming_operations: List):
        with self.lock:
            for op in incoming_operations:
                key, entry = op
                self.perform_operation(key, entry)

    def get_state(self):
        return self.node_state
