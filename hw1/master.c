#include <stdio.h>

#include <stdlib.h>

#include <string.h>

#include <arpa/inet.h>

#include <unistd.h>

#include <netinet/in.h>

#include <sys/socket.h>

#include <sys/select.h>

#include <errno.h>

#include <math.h>

#include "master_service/task_s.h"

#include "master_service/worker_s.h"

#define BROADCAST_PORT 8080
#define BUFFER_S 512

#define BROADCAST_MSG "DISCOVER"
#define WORKERS_LIMIT 5


Worker worker_db[WORKERS_LIMIT];
Task task_db[WORKERS_LIMIT];

void broadcast_search(int udp_socket, struct sockaddr_in * broadcast_addr) {
  char buffer[BUFFER_S];
  snprintf(buffer, BUFFER_S, "%s", BROADCAST_MSG);

  if (sendto(udp_socket, buffer, strlen(buffer), 0, (struct sockaddr * ) broadcast_addr, sizeof( * broadcast_addr)) < 0) {
    perror("Broadcast not completed.\n");
    exit(EXIT_FAILURE);
  }
  printf("Broadcast completed.\n");
  sleep(1);
}

void workers_connecting(int udp_socket, int * con_worker_num) {
  struct sockaddr_in worker_addr;
  socklen_t addr_len = sizeof(worker_addr);
  char buffer[BUFFER_S];

  while (1) {
    int bytes = recvfrom(udp_socket, buffer, BUFFER_S, MSG_DONTWAIT, (struct sockaddr * ) & worker_addr, & addr_len);
    if (bytes > 0) {
      buffer[bytes] = '\0';
      int tcp_port = 0;
      if (sscanf(buffer, "worker %d", & tcp_port) == 1) {
        if ( * con_worker_num < WORKERS_LIMIT) {
          worker_addr.sin_port = htons(tcp_port);
          worker_db[ * con_worker_num].socket = socket(AF_INET, SOCK_STREAM, 0);
          if (connect(worker_db[ * con_worker_num].socket, (struct sockaddr * ) & worker_addr, sizeof(worker_addr)) < 0) {
            close(worker_db[ * con_worker_num].socket);
            perror("Connection to worker failed");
            continue;
          }
          * con_worker_num += 1;

          worker_db[ * con_worker_num].address = worker_addr;
          worker_db[ * con_worker_num].status = 0;

          printf("The worker is connected to port number %d\n", tcp_port);
        }
      }
    } else {
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      else perror("Receive error");
    }
  }
}

int task_assign(Worker * worker, double cur_l, double cur_r, int task_id) {
  char buffer[BUFFER_S];
  snprintf(buffer, BUFFER_S, "%.4f-%.4f", cur_l, cur_r);
  if (send(worker -> socket, buffer, strlen(buffer), 0) < 0) {
    perror("Failed to assign to worker");
    worker -> status = 1;
    return 1;
  }
  worker -> task_id = task_id;

  worker -> status = 1;
  task_db[task_id].taken = 1;
  return 0;
}

void task_forwarding(int * con_worker_num) {
  for (int i = 0; i < * con_worker_num; i++) {
    if (task_db[i].taken == 1) {
      if (!task_db[i].finished) {
        for (int j = 0; j < * con_worker_num; ++j) {
          if (worker_db[j].status == 0) {
            if (!task_assign( & worker_db[j], task_db[i].l, task_db[i].r, i)) break;
          }
        }
      }
    }
  }
}

void result_counting(fd_set * fd_reading_data, int * con_worker_num, int * completed_tasks, double * general_result) {
  char buffer[BUFFER_S];
  for (int i = 0; i < * con_worker_num; i++) {
    if (worker_db[i].status == 1) {
      if (FD_ISSET(worker_db[i].socket, fd_reading_data)) {
        int rec_bytes = recv(worker_db[i].socket, buffer, BUFFER_S, 0);
        if (rec_bytes > 0) {
          worker_db[i].status = 0;
          for (int j = 0; j < * con_worker_num; j++) {
            if (task_db[j].task_id == worker_db[i].task_id) {
              if (!task_db[j].finished) {
                * completed_tasks += 1;
                buffer[rec_bytes] = '\0';
                * general_result += atof(buffer);
                task_db[j].finished = 1;
              }
              break;
            }
          }
        } else {
          worker_db[i].status = -1;
          perror("Receive failed");
        }
      }
    }
  }
}

void manage_tasks(double cur_l, double cur_r, int * con_worker_num, int * completed_tasks, double * general_result) {
  for (int i = 0; i < * con_worker_num; ++i) {
    task_db[i].task_id = i;
    task_db[i].finished = 0;
    task_db[i].taken = 0;

    task_db[i].l = cur_l + i * (cur_r - cur_l) / * con_worker_num;
    task_db[i].r = task_db[i].l + (cur_r - cur_l) / * con_worker_num;
  }

  int processed_tasks_amount = 0;
  int first_iter = 1;
  while ( * con_worker_num > * completed_tasks) {
    fd_set fd_reading_data;
    FD_ZERO( & fd_reading_data);
    int fd_for_select = 0;

    for (int i = 0; i < * con_worker_num; i++) {
      FD_SET(worker_db[i].socket, & fd_reading_data);
      if (fd_for_select < worker_db[i].socket) fd_for_select = worker_db[i].socket;
    }

    if (!first_iter) {
      struct timeval timeout = {
        5,
        0
      };
      int check_actions = select(fd_for_select + 1, & fd_reading_data, NULL, NULL, & timeout);

      if (!check_actions) {
        for (int i = 0; i < * con_worker_num; ++i) {
          if (worker_db[i].status == 1) worker_db[i].status = -1;
        }
        task_forwarding(con_worker_num);
      } else if (check_actions > 0) {
        result_counting( & fd_reading_data, con_worker_num, completed_tasks, general_result);
      }
    }

    for (int i = 0; processed_tasks_amount < * con_worker_num && i < * con_worker_num; i++) {
      if (!worker_db[i].status) {
        if (!task_db[processed_tasks_amount].taken) {
          task_assign( & worker_db[i], task_db[processed_tasks_amount].l, task_db[processed_tasks_amount].r, processed_tasks_amount);
        }
        processed_tasks_amount += 1;
      }
    }
    first_iter = 0;
  }
}

int main() {
  int con_worker_num = 0;
  int completed_tasks = 0;
  double general_result = 0.0;

  int broadc_socket = socket(AF_INET, SOCK_DGRAM, 0);
  if (broadc_socket < 0) {
    perror("Problem with udp socket");
    exit(EXIT_FAILURE);
  }

  int broadcast_enable = 1;
  setsockopt(broadc_socket, SOL_SOCKET, SO_BROADCAST, & broadcast_enable, sizeof(broadcast_enable));
  struct sockaddr_in broadcast_addr;
  memset( & broadcast_addr, 0, sizeof(broadcast_addr));
  broadcast_addr.sin_family = AF_INET;
  broadcast_addr.sin_port = htons(BROADCAST_PORT);
  broadcast_addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);

  broadcast_search(broadc_socket, & broadcast_addr);
  workers_connecting(broadc_socket, & con_worker_num);
  close(broadc_socket);
  if (!con_worker_num) {
    printf("No workers!.\n");
    exit(EXIT_FAILURE);
  }
  double l = 0, r = 1;
  manage_tasks(l, r, & con_worker_num, & completed_tasks, & general_result);
  printf("Result %f\n", general_result);
  for (int soc = 0; soc < con_worker_num; soc++) {
    close(worker_db[soc].socket);
  }
  return 0;
}