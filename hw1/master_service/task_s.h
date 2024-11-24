#ifndef TASK_S_H
#define TASK_S_H

#define WORKERS_LIMIT 5

typedef struct {
    int task_id;
    double l;
    double r;
    int finished;
    int taken;
} Task;

#endif // TASK_S_H

