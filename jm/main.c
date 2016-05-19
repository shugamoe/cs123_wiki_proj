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
    // waitFor(2);
    int i;
    printf("Creating products\n");
    for (i = 0; i < 500; i++) {
        /* produce something */
        /* append it to a list */
        c++;
    }
    printf("Now %d products\n", c);
    pthread_mutex_lock(&m);
    printf("Locked_p\n");
    done = true;
    pthread_cond_signal(&cond_var);
    printf("Signal Sent\n");
    pthread_mutex_unlock(&m);
    printf("Unlocked_p\n");
    return NULL;
}

void* consumer(void* unused) {
    printf("Consumer start\n");
    // waitFor(2);
    pthread_mutex_lock(&m);
    printf("Locked_c\n");
    while (!done) {
        printf("Checking\n");
        pthread_cond_wait(&cond_var, &m);
    }
    printf("Will now consume products\n");
    while (c > 0) {
        /* remove something from list */
        c--;
    }
    pthread_mutex_unlock(&m);
    printf("Unlocked_c\n");
    return NULL;
}

int main(int argc, char** argv) {
    pthread_t prod, con;

    printf("Creating Threads\n");
    pthread_create(&prod, NULL, producer, NULL);
    pthread_create(&con, NULL, consumer, NULL);

    void* unused;
    printf("Joining threads\n");
    pthread_join(prod, &unused);
    pthread_join(con, &unused);
    printf("Threads Finished\n");

    printf("Net: %d\n", c);
}