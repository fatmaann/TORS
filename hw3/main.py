import sys
import threading
import time
from typing import Dict, List, Tuple

import requests
from fastapi import FastAPI
import uvicorn
from node_status import SECMap

app = FastAPI()
node = SECMap()


@app.get("/state")
def get_state(key: str = None):
    state = node.get_state()
    if key:
        entry = state.get(key)
        if entry:
            return {key: entry["val"]}
        else:
            return {"key": "not found"}
    return state


@app.patch("/update")
async def patch_state(key: str, val: str = None):
    if val is None:
        operations = [node.delete(key)]
    else:
        operations = [node.add(key, val)]

    for idx, (peer_url, alive) in enumerate(all_repl):
        if alive:
            try:
                response = requests.post(f"{peer_url}/sync", json=[operations[0]])
                if response.status_code == 200:
                    print(f"Successfully synced with {peer_url}")
                    all_repl[idx] = (peer_url, True)
                else:
                    print(f"Failed to sync with {peer_url}: {response.text}")
                    all_repl[idx] = (peer_url, False)
            except Exception as e:
                print(f"Failed to sync with {peer_url}: {e}")
                all_repl[idx] = (peer_url, False)

    return {"operations": operations}


@app.post("/sync")
def sync_node(incoming_operations: List[Tuple[str, Dict[str, object]]] = []):
    if incoming_operations:
        node.merge(incoming_operations)
    return {"status": "synced"}


def try_to_sync_dead_nodes():
    while True:
        operations = list(node.get_state().items())
        for idx, (peer_url, alive) in enumerate(all_repl):
            if not alive:
                try:
                    response = requests.post(f"{peer_url}/sync", json=operations)
                    if response.status_code == 200:
                        print(f"Successfully synced full state with {peer_url}")
                        all_repl[idx] = (peer_url, True)
                    else:
                        print(
                            f"Failed to sync full state with {peer_url}"
                        )
                        all_repl[idx] = (peer_url, False)
                except Exception as e:
                    print(f"Failed to sync with {peer_url}")
                    all_repl[idx] = (peer_url, False)
        time.sleep(0.1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(1)
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    all_repl = [(f"http://localhost:{8000 + i}", True) for i in range(0, 4) if i != node_id]
    node.node_conf(node_id=node_id, repls_amount=len(all_repl) + 1)
    threading.Thread(target=try_to_sync_dead_nodes, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=port)
