#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>

void waitFor (unsigned int secs) {
    unsigned int retTime = time(0) + secs;   // Get finishing time.
    while (time(0) < retTime);               // Loop until it arrives.
}

pthread_cond_t cond_var = PTHREAD_COND_INITIALIZER;
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

int c = -5;
bool done = false;

void* producer(void* unused) {
    printf("Producer Start\n");
    int i;
    for (i = 0; i < 500; i++) {
        /* produce something */
        /* append it to a list */
        pthread_mutex_lock(&m);
        // printf("Locked_p\n");
        c++;
        printf("+1");
        pthread_cond_signal(&cond_var);
        // printf("Signal Sent\n");
        pthread_mutex_unlock(&m);
        // printf("Unlocked_p\n");
    }
    pthread_mutex_lock(&m);
    done = true; 
    printf("\tProduction Complete\n");
    pthread_cond_broadcast(&cond_var);  
    pthread_mutex_unlock(&m); 
    return NULL;
}


void* consumer(void* unused) {
    printf("Consumer start\n");
    pthread_mutex_lock(&m);
    while (!done) {
        printf("Checking\n");
        pthread_cond_wait(&cond_var, &m);
        while (c > 0) {
        /* remove something from list */
            c--;
            printf("-1");
            pthread_mutex_unlock(&m);
            pthread_mutex_lock(&m);
        }
        pthread_mutex_unlock(&m);
        pthread_mutex_lock(&m);
    }
    pthread_mutex_unlock(&m);
    while (c > 0) {
        /* remove something from list */
        c--;
    }
    return NULL;
}


int main(int argc, char** argv) {
    int num_con = atoi(argv[1]);
    pthread_t* con = (pthread_t*)malloc(sizeof(pthread_t)*num_con); 
    pthread_t prod;
    int i;

    printf("Creating Threads\n");
    pthread_create(&prod, NULL, producer, NULL);
    for (i = 1; i <= num_con; i++) {
        pthread_create(&con[i], NULL, consumer, NULL);
    }
    

    void* unused;
    pthread_join(prod, &unused);

    for (i = 1; i <= num_con; i++) {
        void* noneed;
        pthread_join(con[i], &noneed);
    }
    printf("Threads Finished\n");
    free(con);
    printf("Net: %d\n", c);
}