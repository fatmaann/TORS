#ifndef WORKER_S_H
#define WORKER_S_H

#include <netinet/in.h>

#define WORKERS_LIMIT 5

typedef struct {
    int socket;
    int task_id;
    int status;
    struct sockaddr_in address;
} Worker;

#endif // WORKER_S_H
