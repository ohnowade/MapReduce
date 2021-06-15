#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include "mapreduce.h"

pthread_mutex_t lock;

typedef struct {
    pthread_t p;
    int thread_id;
    int use;
} thread_info;

// The table storing all keys and their values for combiners and reducers
typedef struct {
    char*** data; // the table storing all keys and values
    int num_key; // the number of keys in the table
    int* num_values; // the number of values for each key in the table
} table;

// the collection of all combiners' tables
table* combiner_tables = NULL;
// the collection of all reducers' partition tables
table* partitions = NULL;

// the partition function to be used
Partitioner user_partitioner;
// the mapper function to be used
Mapper user_mapper;
// the combiner function to be used
Combiner user_combiner;
// the reducer function to be used
Reducer user_reducer;

// the collection of all mapper threads used for locating id of a single thread
thread_info* map_manager;

int map_num; // the number of mappers
int red_num; // the number of reducers

// pointers used by combiner get_next
char*** cg_pointer;
// counter used by combiner get_next
int* cg_count;
// indices used by reducer get_next
int* rp_index;
// pointers used by reducer get_next
char*** rp_pointer;

/**
 * get the corresponding id for current thread
 */
int get_thread_id() {
    for (int i = 0; i < map_num; i++) {
        if (map_manager[i].p == pthread_self()) return map_manager[i].thread_id;
    }

    return -1;
}

/**
 * gets the table owned by current thread
 */ 
table* get_table() {
   return &combiner_tables[get_thread_id()];
}

/**
 * free the table owned by current thread when it is complete
 */
void free_table() {
    table* cur_table = get_table();
    for (int i = 0; i < cur_table->num_key; i++) {
        for (int j = 0; j <= cur_table->num_values[i]; j++) {
            free(cur_table->data[i][j]);
        }
        free(cur_table->data[i]);
    }
    free(cur_table->data);
    free(cur_table->num_values);
    cur_table->data = NULL;
    cur_table->num_values = NULL;
    cur_table->num_key = 0;
}

/**
 * free the partition table used by current reducer whose partition number is passed in
 */
void free_p_table(int partition_number) {
    table* cur_table = &partitions[partition_number];

    for (int i = 0; i < cur_table->num_key; i++) {
        for(int j = 0; j <= cur_table->num_values[i]; j++) {
            free(cur_table->data[i][j]);
        }
        free(cur_table->data[i]);
    }
    free(cur_table->data);
    cur_table->data = NULL;
    free(cur_table->num_values);
    cur_table->num_values = NULL;
    cur_table->num_key = 0;
}

void MR_EmitToCombiner(char *key, char *value) {
    table* cur_table = get_table();

    for (int i = 0; i < cur_table->num_key; i++) {
        if (!strcmp(cur_table->data[i][0], key)) {
            // If the key is already present in the table, add the key to its value list
            (cur_table->num_values[i])++;
            cur_table->data[i] = realloc(cur_table->data[i], (cur_table->num_values[i] + 1) * sizeof(char*));
            cur_table->data[i][cur_table->num_values[i]] = strdup(value);
            return;
        }
    }

    // the function not returning means the key is not present in the table
    // create a new entry in the table for the key
    (cur_table->num_key)++;
    cur_table->data = realloc(cur_table->data, sizeof(char**) * cur_table->num_key);
    cur_table->num_values = realloc(cur_table->num_values, sizeof(int) * cur_table->num_key);
    cur_table->num_values[cur_table->num_key - 1] = 1;
    cur_table->data[cur_table->num_key - 1] = malloc(sizeof(char*) * 2);
    cur_table->data[cur_table->num_key - 1][0] = strdup(key);
    cur_table->data[cur_table->num_key - 1][1] = strdup(value);
}

/**
 *  the get_next to be passed to combiner
 */
char* combiner_get_next(char* key) {
    int tid = get_thread_id();

    if (cg_pointer[tid] == NULL) {
        table* cur_table = get_table();
        for (int i = 0; i < cur_table->num_key; i++) {
            if (!strcmp(key, cur_table->data[i][0])) {
                cg_count[tid] = cur_table->num_values[i];
                cg_pointer[tid] = &(cur_table->data[i][0]);
                break;
            }
        }
    }

    if (cg_count[tid] > 0) {
        cg_pointer[tid]++;
        cg_count[tid]--;
        return *(cg_pointer[tid]);
    } else {
        cg_pointer[tid] = NULL;
        cg_count[tid] = 0;
        return NULL;
    }
}

void MR_EmitToReducer(char *key, char *value) {
    pthread_mutex_lock(&lock);
    // get the partition number for the key
    int p_num = user_partitioner(key, red_num);
    // get its corresponding table
    table* cur_table = &partitions[p_num];

    for (int i = 0; i < cur_table->num_key; i++) {
        if (!strcmp(cur_table->data[i][0], key)) {
            // if the key is already present in its partition table
            (cur_table->num_values[i])++;
            cur_table->data[i] = realloc(cur_table->data[i], sizeof(char*) * (cur_table->num_values[i] + 1));
            cur_table->data[i][cur_table->num_values[i]] = strdup(value);
            pthread_mutex_unlock(&lock);
            return;
        }
    }

    // if the key has not been present in its partition table, create an entry for it
    (cur_table->num_key)++;
    cur_table->data = realloc(cur_table->data, cur_table->num_key * sizeof(char**));
    cur_table->num_values = realloc(cur_table->num_values, cur_table->num_key * sizeof(int));
    cur_table->num_values[cur_table->num_key - 1] = 1;
    cur_table->data[cur_table->num_key - 1] = malloc(sizeof(char*) * 2);
    cur_table->data[cur_table->num_key - 1][0] = strdup(key);
    cur_table->data[cur_table->num_key - 1][1] = strdup(value);
    pthread_mutex_unlock(&lock);
}

/**
 * the get_next function to be passed to reducer
 */
char* reducer_get_next(char* key, int partition_number) {
    table* cur_table = &partitions[partition_number];

    if (rp_pointer[partition_number] == NULL) {
        for (int i = 0; i < cur_table->num_key; i++) {
            if (!strcmp(cur_table->data[i][0], key)) {
                rp_pointer[partition_number] = &(cur_table->data[i][0]);
                rp_index[partition_number] = cur_table->num_values[i];
                break;
            }
        }
    } 

    if (rp_index[partition_number] > 0) {
        rp_pointer[partition_number]++;
        rp_index[partition_number]--;
        return *(rp_pointer[partition_number]);
    } else {
        rp_pointer[partition_number] = NULL;
        rp_index[partition_number] = 0;
        return NULL;
    }
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void* mapper_wrapper(void* map_args) {
    // call mapper with the file passed in
    user_mapper((char*)map_args);

    if (user_combiner != NULL) {
        table* cur_table = get_table();

        for (int i = 0; i < cur_table->num_key; i++) { 
            user_combiner(cur_table->data[i][0], &combiner_get_next);
        }
    }

    free_table();
    return NULL;
}

void* reducer_wrapper(void* red_args) {
    int p_num = *((int*)(red_args));

    table* cur_table = &partitions[p_num];

    for (int i = 0; i < cur_table->num_key; i++) {
        user_reducer(cur_table->data[i][0], NULL, &reducer_get_next, p_num);
    }

    return NULL;
}

int comparator(const void* p1, const void* p2) {
    char** first = (char**)p1;
    char** second = (char**)p2;
    return strcmp(*first, *second);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Combiner combine, Partitioner partition) {
    pthread_mutex_init(&lock, NULL);

    user_mapper = map;
    user_combiner = combine;
    user_partitioner = partition;
    user_reducer = reduce;

    thread_info map_threads[num_mappers];
    pthread_t reduce_threads[num_reducers];  
    map_manager = &map_threads[0];
    for (int i = 0; i < num_mappers; i++) {
        map_threads[i].thread_id = i;
        map_threads[i].use = 0;
    }

    // create and initialize tables for combiners
    combiner_tables = malloc(sizeof(table) * num_mappers);
    map_num = num_mappers;
    for (int i = 0; i < num_mappers; i++) {
        combiner_tables[i].data = NULL;
        combiner_tables[i].num_key = 0;
        combiner_tables[i].num_values = NULL;
    }
    // create and initialize partitions for reducers
    partitions = malloc(sizeof(table) * num_reducers);
    red_num = num_reducers;
    for (int i = 0; i < num_reducers; i++) {
        partitions[i].data = NULL;
        partitions[i].num_key = 0;
        partitions[i].num_values = NULL;
    }

    // allocate memory for pointers and counters used in get_next() functions
    cg_pointer = malloc(sizeof(char**) * map_num);
    for (int i = 0; i < map_num; i++) cg_pointer[i] = NULL;
    cg_count = malloc(sizeof(int) * map_num);
    for (int i = 0; i < map_num; i++) cg_count[i] = 0;
    rp_index = malloc(sizeof(int) * red_num);
    for (int i = 0; i < red_num; i++) rp_index[i] = 0;
    rp_pointer = malloc(sizeof(char**) * red_num);
    for (int i = 0; i < red_num; i++) rp_pointer[i] = NULL;   
    
    int file_num = argc - 1; // the number of files 

    while (file_num > 0) {
        for (int i = 0; i < num_mappers; i++) {
            if (file_num > 0) {
                map_threads[i].use = 1;
                pthread_create(&map_threads[i].p, NULL, mapper_wrapper, (void*)argv[argc - file_num]);
                file_num--;
            } else {
                // If there are no more files to be processed, remaining threads will idle
                break;
            }
        }
        // wait for all mapper threads to finish
        for (int i = 0; i < num_mappers; i++) {
            if (map_threads[i].use) {
                pthread_join(map_threads[i].p, NULL);
                map_threads[i].use = 0;
            }
        }
    }
    free(combiner_tables);
    combiner_tables = NULL;

    for (int i = 0; i < num_reducers; i++) {
        printf("Partition %d:\n", i);
        table* cur_table = &partitions[i];
        for (int j = 0; j < cur_table->num_key; j++) {
            printf("[%s: ", cur_table->data[j][0]);
            for (int k = 1; k <= cur_table->num_values[j]; k++) {
                printf("%s, ", cur_table->data[j][k]);
            }
            printf("]\n");
        }
    }

    printf("\n");

    for (int i = 0; i < num_reducers; i++) {
        table* cur_table = &partitions[i];
        qsort(cur_table->data, cur_table->num_key, sizeof(char**), comparator);
    }

    for (int i = 0; i < num_reducers; i++) {
        printf("Partition %d:\n", i);
        table* cur_table = &partitions[i];
        for (int j = 0; j < cur_table->num_key; j++) {
            printf("[%s: ", cur_table->data[j][0]);
            for (int k = 1; k <= cur_table->num_values[j]; k++) {
                printf("%s, ", cur_table->data[j][k]);
            }
            printf("]\n");
        }
    }
    
    // the partition numbers to be passed in to each reducers
    int* p_nums = malloc(sizeof(int) * red_num);
    for (int i = 0; i < red_num; i++) *(p_nums + i) = i;

    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reduce_threads[i], NULL, reducer_wrapper, (void*)(p_nums + i));
    }

    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reduce_threads[i], NULL);
        free_p_table(i);
    }
    free(p_nums);
    free(partitions);
    partitions = NULL;
    free(cg_count);
    free(cg_pointer);
    free(rp_index);
    free(rp_pointer);
    return;
}



