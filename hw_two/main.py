from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.responses import JSONResponse, RedirectResponse
import threading
import requests
import random
from src_cas import update_db_by_cas, get_from_db, set_from_db, pop_from_db
from node_status import main_host, RaftNode
import uvicorn
import sys
import time

node_cas_lock = threading.Lock()
node_status = RaftNode()
app = FastAPI()


@app.get("/data/{key}")
async def get_item(key: str):
    if node_status.role == "master":
        alive_repl = [r for r in node_status.network if r[1] != 0]
        if alive_repl:
            replica = random.choice(alive_repl)
            return Response(content=f"{replica[0]}/data/{key}", status_code=302)
        else:
            value = get_from_db(node_status.data, key, node_cas_lock)
            entry = {"operation": "post", "key": key, "value": value}
            node_status.log.append(entry)
            return JSONResponse(content=get_from_db(node_status.data, key, node_cas_lock), status_code=200)
    else:
        return JSONResponse(content=get_from_db(node_status.data, key, node_cas_lock), status_code=200)


@app.post("/data/")
async def create_item(key: str, value: str):
    if node_status.role == "master":
        success = append_log_and_replicate_with_majority({"operation": "post", "key": key, "value": value})
        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, post")
    else:
        raise HTTPException(status_code=500, detail="The request was sent not to the master node, post")


@app.put("/data/{key}")
async def update_item(key: str, value: str):
    if node_status.role == "master":
        success = append_log_and_replicate_with_majority({"operation": "put", "key": key,
                                                          "old_value": get_from_db(node_status.data, key,
                                                                                   node_cas_lock),
                                                          "value": value})
        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, put")
    else:
        raise HTTPException(status_code=500, detail="The request was sent not to the master node, put")


@app.patch("/data/{key}")
async def partial_update_item(key: str, value: str):
    if node_status.role == "master":
        success = append_log_and_replicate_with_majority({"operation": "patch", "key": key,
                                                          "old_value": get_from_db(node_status.data, key,
                                                                                   node_cas_lock),
                                                          "value": value})
        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, patch")
    else:
        raise HTTPException(status_code=500, detail="The request was sent not to the master node, patch")


@app.delete("/data/{key}")
async def delete_item(key: str):
    if node_status.role == "master":
        success = append_log_and_replicate_with_majority({"operation": "delete", "key": key})
        if success:
            return JSONResponse(content={"status": "success"}, status_code=200)
        else:
            raise HTTPException(status_code=500, detail="Majority not reached, delete")
    else:
        raise HTTPException(status_code=500, detail="The request was sent not to the master node, delete")


def append_log_and_replicate_with_majority(entry):
    checking_key_for_com = True
    node_status.log.append(entry)
    if entry["operation"] != "delete":
        if entry["operation"] != "post":
            checking_key_for_com = update_db_by_cas(node_status.data, entry["key"],
                                                    entry["old_value"],
                                                    entry["value"],
                                                    node_cas_lock)
        else:
            checking_key_for_com = set_from_db(node_status.data,
                                               entry["key"],
                                               entry.get("value"),
                                               node_cas_lock)
    else:
        pop_from_db(node_status.data, entry["key"], node_cas_lock)

    conf_election_votes = 1
    network_majority = len(node_status.network) // 2

    def replicate_data(node_link):
        nonlocal conf_election_votes
        try:
            response = requests.post(
                f"{node_link}/replicate",
                json=entry,
                headers={"Master-Term": str(node_status.term)},
                timeout=2,
            )
            if response.status_code == 200:
                conf_election_votes += 1
        except requests.exceptions.RequestException:
            pass

    if checking_key_for_com:
        threads = []
        for repl in node_status.network:
            thread = threading.Thread(target=replicate_data, args=(repl[0],))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        return conf_election_votes >= network_majority
    return checking_key_for_com


@app.post("/replicate")
async def handle_replication(entry: dict, request: Request):
    master_term = int(request.headers.get("Master-Term", -1))

    if master_term < node_status.term:
        return JSONResponse(
            content={"status": "rejected", "reason": "Term mismatch"},
            status_code=400,
        )

    if master_term > node_status.term:
        node_status.term = master_term

    node_status.log.append(entry)
    if entry["operation"] != "delete":
        if entry["operation"] != "post":
            update_db_by_cas(node_status.data, entry["key"], entry["old_value"], entry["value"], node_cas_lock)
        else:
            set_from_db(node_status.data, entry["key"], entry["value"], node_cas_lock)
    else:
        pop_from_db(node_status.data, entry["key"], node_cas_lock)

    return JSONResponse(content={"status": "ok"}, status_code=200)


@app.post("/election")
async def handle_election(request: Request):
    body = await request.json()
    term = body.get("term")

    if term > node_status.term:
        node_status.processing_election(term)
        return JSONResponse(content={"vote_granted": True}, status_code=200)
    return JSONResponse(content={"vote_granted": False}, status_code=200)


def start_election():
    answers_amount = 0

    def request_vote(node_link):
        nonlocal answers_amount
        try:

            response = requests.post(
                f"{node_link}/election",
                json=node_status.candidate_json(),
                timeout=1,
            )

            if response.status_code == 200:
                answers_amount += 1
                if response.json().get("vote_granted"):
                    node_status.election_votes += 1

        except requests.exceptions.RequestException:
            pass

    threads = []
    node_status.term += 1
    node_status.election_votes = 1

    node_status.role = "candidate"
    node_status.raft_master = None
    for repl in node_status.network:
        if repl[1] != 0:
            thread = threading.Thread(target=request_vote, args=(repl[0],))
            thread.start()
            threads.append(thread)

    for thread in threads:
        thread.join()

    if node_status.election_votes >= (answers_amount + 1) // 2 + 1:
        print(node_status.port, 'стал мастером')
        node_status.becoming_master()
        threading.Thread(target=send_heartbeat, daemon=True).start()
    else:
        node_status.still_follower()


def monitor_timeouts():
    while True:
        if (node_status.role == "follower") and ((time.time() - node_status.heartbeat) > node_status.re_election):
            start_election()
        time.sleep(2)


@app.post("/heartbeat")
async def handle_heartbeat(request: Request):
    body = await request.json()
    term = body.get("term")
    master_id = body.get("master_id")
    master_data = body.get("data", {})
    log_data = body.get("log_data", {})
    if term >= node_status.term:
        node_status.processing_hb(master_id, term, master_data, log_data)
        return JSONResponse(content={"status": "ok"}, status_code=200)
    return JSONResponse(content={"status": "rejected"}, status_code=400)


def send_heartbeat():
    while node_status.role == "master":
        for repl_id in range(len(node_status.network)):
            try:
                response = requests.post(
                    f"{node_status.network[repl_id][0]}/heartbeat",
                    json=node_status.heartbeat_json(),
                    timeout=1,
                )

                if response.status_code in [200, 400]:
                    node_status.network[repl_id][1] = 1
                else:
                    node_status.network[repl_id][1] = 0

            except requests.exceptions.RequestException:
                node_status.network[repl_id][1] = 0
                pass

        time.sleep(0.1)


if __name__ == "__main__":
    port = int(sys.argv[1])
    node_status.port = port
    node_status.network.remove([f"{main_host}{port}", 1])
    threading.Thread(target=monitor_timeouts, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=port)
