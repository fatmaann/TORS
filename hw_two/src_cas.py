def get_from_db(node_data, key, lock):
    with lock:
        return node_data.get(key, None)


def set_from_db(node_data, key, value, lock):
    with lock:
        current_value = node_data.get(key, None)
        if current_value:
            return False
        node_data[key] = value
        return True


def pop_from_db(node_data, key, lock):
    with lock:
        return node_data.pop(key, None)


def update_db_by_cas(node_data, key, expected_value, new_value, lock):
    with lock:
        current_value = node_data.get(key, None)
        if current_value and current_value == expected_value:
            node_data[key] = new_value
            return True
        return False
