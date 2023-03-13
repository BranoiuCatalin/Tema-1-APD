#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_STRING_LENGTH 50
#define MAX_NUM_OF_INTS 500001

pthread_mutex_t alloc_new_file_mtx;
pthread_barrier_t wait_for_mappers_bar;

typedef struct argument_set {
  int id;
  int nr_map_threads, nr_reduce_threads;
  int file_num;
  int *current_file_index;
  FILE **processing_files;
  int ***partial_lists;

} argument_set;

int is_perfect_power(int num, int power) {
  int start = 2;
  int end = num;

  if (num == 1) return 1;

  while (start < end) {
    int middle = 1 + (start + end) / 2;
    if (pow(middle, power) == num) {
      return 1;
    } else if (pow(middle, power) < num) {
      start = middle + 1;
    } else if (pow(middle, power) > num) {
      end = middle - 1;
    }
  }

  if (start == end && pow(start, power) == num) {
    return 1;
  }

  return 0;
}

void *paralel_func(void *arg) {
  FILE *current_fd;
  argument_set argument = *(argument_set *)arg;
  int ***partial_lists = argument.partial_lists;
  //printf("Salut din threadul %d \n", argument.id);

  if (argument.id < argument.nr_map_threads) {
    // printf("dx %d  ..  %d\n",*argument.current_file_index,
    // argument.file_num);
    
    while (*argument.current_file_index < argument.file_num) {
      pthread_mutex_lock(&alloc_new_file_mtx);
      if (argument.id < argument.nr_map_threads) {
      // printf("mm\n");
      current_fd = argument.processing_files[*argument.current_file_index];
      // //printf("Threadul %d a luat fisierul %d si a incerementat la %d\n",
      //        argument.id, *argument.current_file_index,
      //        *argument.current_file_index + 1);
      *argument.current_file_index += 1;
      pthread_mutex_unlock(&alloc_new_file_mtx);}

      int num_of_nums;
      fscanf(current_fd, "%d", &num_of_nums);
      //printf("%d\n", num_of_nums);

      for (int i = 0; i < num_of_nums; i++) {
        int current_num;
        fscanf(current_fd, "%d", &current_num);
        for (int j = 0; j < argument.nr_reduce_threads; j++) {
          if (is_perfect_power(current_num, j + 2)) {
            partial_lists[argument.id][j][0]++;
            partial_lists[argument.id][j][partial_lists[argument.id][j][0]] =
                current_num;
          }
        }
      }
      //printf("\n");
      fclose(current_fd);
    }
  }

  // bariera map-thread
  pthread_barrier_wait(&wait_for_mappers_bar);

  if (argument.id < argument.nr_reduce_threads) {
    int *reducer_list;
    reducer_list = calloc(sizeof(int), MAX_NUM_OF_INTS);

    for (int i = 0; i < argument.nr_map_threads; i++) {
      for (int j = 1; j <= partial_lists[i][argument.id][0]; j++) {
        int num_found = 0;
        for(int k = 1; k<= reducer_list[0]; k++) {
          if(reducer_list[k] == partial_lists[i][argument.id][j]) {
            num_found = 1;
          }
        }
        if(!num_found) {
          reducer_list[0]++;
          reducer_list[reducer_list[0]] = partial_lists[i][argument.id][j];
        }
      }
    }
    
    //pthread_mutex_lock(&alloc_new_file_mtx);
    //printf("Lista redusa a threadului %d este:\n", argument.id);
    //for(int i=1; i<=reducer_list[0]; i++) {
      //printf("%d ", reducer_list[i]);
    //}
    //printf("\n");

    char filename[MAX_STRING_LENGTH];
    char out_file_num[MAX_STRING_LENGTH];
    sprintf(out_file_num, "%d", argument.id+2);
    strcpy(filename, "out");
    strcat(filename, out_file_num);
    //strcat(filename, "_par");
    strcat(filename, ".txt");

    //printf("Se va scrie in fila cu numele %s\n", filename);
    //pthread_mutex_unlock(&alloc_new_file_mtx);

    FILE *out_file;
    out_file = fopen(filename, "w");
    fprintf(out_file,"%d",reducer_list[0]);
    fclose(out_file);

    free(reducer_list);

  }
  pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
  pthread_mutex_init(&alloc_new_file_mtx, NULL);
  // parsare argumente
  if (argc < 4) {
    perror("Specificati dimensiunea array-ului\n");
    exit(-1);
  }
  int nr_map_threads = 0, nr_reduce_threads = 0;
  nr_map_threads = atoi(argv[1]);
  nr_reduce_threads = atoi(argv[2]);
  char *init_file_name = (char *)malloc((strlen(argv[3]) + 1) * sizeof(char));
  strcpy(init_file_name, argv[3]);

  // parsare fisier argument
  FILE *initial_file;

  initial_file = fopen(init_file_name, "r");
  int num_of_files = 0;
  fscanf(initial_file, "%d", &num_of_files);

  // creare file descriptori pentru fisierele de intrare
  FILE **processing_files = malloc(sizeof(FILE *) * (num_of_files));

  for (int i = 0; i < num_of_files; i++) {
    char proc_file[MAX_STRING_LENGTH];
    fscanf(initial_file, "%s", proc_file);
    processing_files[i] = fopen(proc_file, "r");
  }
  fclose(initial_file);

  int ***partial_lists;
  partial_lists = malloc(sizeof(int **) * nr_map_threads);
  for (int i = 0; i < nr_map_threads; i++) {
    partial_lists[i] = malloc(sizeof(int *) * nr_reduce_threads);
    for (int j = 0; j < nr_reduce_threads; j++) {
      partial_lists[i][j] = calloc(MAX_NUM_OF_INTS, sizeof(int));
    }
  }

  int max_threads = fmax(nr_map_threads, nr_reduce_threads);
  pthread_t threads[max_threads];
  pthread_barrier_init(&wait_for_mappers_bar, NULL, max_threads);
  int r;
  void *status;
  int current_file_index = 0;

  argument_set arg[max_threads];
  for (int i = 0; i < max_threads; i++) {
    arg[i].id = i;
    arg[i].nr_map_threads = nr_map_threads;
    arg[i].nr_reduce_threads = nr_reduce_threads;
    arg[i].file_num = num_of_files;
    arg[i].current_file_index = &current_file_index;
    arg[i].partial_lists = partial_lists;
    arg[i].processing_files = processing_files;
  }

  // creare threaduri
  for (int id = 0; id < max_threads; id++) {
    r = pthread_create(&threads[id], NULL, paralel_func, &arg[id]);

    if (r) {
      printf("Eroare la crearea thread-ului %d\n", id);
      exit(-1);
    }
  }

  // unire threaduri
  for (int id = 0; id < max_threads; id++) {
    r = pthread_join(threads[id], &status);

    if (r) {
      printf("Eroare la asteptarea thread-ului %d\n", id);
      exit(-1);
    }
  }

  //printf("%d\n", is_perfect_power(243, 5));

  for (int i = 0; i < nr_map_threads; i++) {
    for (int j = 0; j < nr_reduce_threads; j++) {
      for (int k = 1; k <= partial_lists[i][j][0]; k++) {
        //printf("%d ", partial_lists[i][j][k]);
      }
      //printf("    ");
    }
    //printf("\n");
  }

  for (int i = 0; i < nr_map_threads; i++) {
    for (int j = 0; j < nr_reduce_threads; j++) {
      free(partial_lists[i][j]);
    }
    free(partial_lists[i]);
  }
  free(partial_lists);

  free(processing_files);
  free(init_file_name);

  pthread_barrier_destroy(&wait_for_mappers_bar);
  pthread_mutex_destroy(&alloc_new_file_mtx);
}