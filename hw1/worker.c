#include <pthread.h>

#include <errno.h>

#include <arpa/inet.h>

#include <unistd.h>

#include <string.h>

#include <math.h>

#include <stdio.h>

#include <stdlib.h>

#define BUFFER_S 512
#define UDP_PORT 8080

void answer_UDP(int udp, int tcp, struct sockaddr_in * main_adress) {
  char answer[BUFFER_S];
  snprintf(answer, BUFFER_S, "worker %d", tcp);
  sendto(udp, answer, strlen(answer), 0, (struct sockaddr * ) main_adress, sizeof( * main_adress));
}

double math_function(double x) {
  return tan(x);
}

double calculate(double left, double right, int steps) {
  double step_size = (right - left) / steps;
  double integral = 0.0;

  for (int i = 0; i < steps; ++i) {
    double x0 = left + i * step_size;
    double x1 = x0 + step_size;
    integral += (math_function(x0) + math_function(x1)) * step_size / 2.0;
  }

  return integral;
}

void * ask_to_calculate(void * args) {
  char buffer[BUFFER_S];
  int socket = * (int * ) args;

  free(args);
  while (1) {
    int bytes = recv(socket, buffer, BUFFER_S - 1, 0);
    if (bytes <= 0) break;

    buffer[bytes] = '\0';
    double l = 0, r = 0;
    sscanf(buffer, "%lf-%lf", & l, & r);
    printf("Task got from master: [%.4f, %.4f]\n", l, r);

    double result = calculate(l, r, 5000);

    snprintf(buffer, BUFFER_S, "%.6f", result);
    if (send(socket, buffer, strlen(buffer), 0) >= 0) printf("Sent result");
    else break;
  }

  close(socket);
  return NULL;
}

int main(int argc, char * argv[]) {
  if (argc != 2) {
    exit(EXIT_FAILURE);
  }
  int port = atoi(argv[1]);

  int udp = socket(AF_INET, SOCK_DGRAM, 0);
  int r = 1;
  setsockopt(udp, SOL_SOCKET, SO_REUSEADDR, & r, sizeof(r));
  struct sockaddr_in udp_address;
  memset( & udp_address, 0, sizeof(udp_address));
  udp_address.sin_family = AF_INET;
  udp_address.sin_port = htons(UDP_PORT);
  udp_address.sin_addr.s_addr = htonl(INADDR_ANY);
  bind(udp, (struct sockaddr * ) & udp_address, sizeof(udp_address));

  int tcp = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in tcp_addr;
  memset( & tcp_addr, 0, sizeof(tcp_addr));
  tcp_addr.sin_family = AF_INET;
  tcp_addr.sin_port = htons(port);
  tcp_addr.sin_addr.s_addr = INADDR_ANY;

  bind(tcp, (struct sockaddr * ) & tcp_addr, sizeof(tcp_addr));
  listen(tcp, 15);

  while (1) {
    struct sockaddr_in main_adress;
    socklen_t addr_len = sizeof(main_adress);
    char buffer[BUFFER_S];

    int bytes = recvfrom(udp, buffer, BUFFER_S - 1, 0, (struct sockaddr * ) & main_adress, & addr_len);
    if (bytes > 0) {
      buffer[bytes] = '\0';
      if (strcmp(buffer, "DISCOVER") == 0) answer_UDP(udp, port, & main_adress);
    }

    int socket = accept(tcp, NULL, NULL);

    pthread_t thread_calculate;
    int * t_calculate = malloc(sizeof(int));
    * t_calculate = socket;

    if (pthread_create( & thread_calculate, NULL, ask_to_calculate, t_calculate) == 0) {
      pthread_detach(thread_calculate);
    } else {
      free(t_calculate);
      close(socket);
    }
  }

  close(tcp);
  close(udp);
  return 0;
}