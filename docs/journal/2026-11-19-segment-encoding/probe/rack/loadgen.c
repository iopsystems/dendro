/*
 * loadgen.c: thread and cgroup churn for the segment-encoding probe.
 *
 * Reproduces the synthetic load described in rezolus's
 * docs/journal/2026-09-25-dendro-archive-layout.md, "A synthetic spike":
 *
 *   thread-per-request: THREADS_PER_SEC new threads a second, each named
 *     into one of four pools ("pool-a" .. "pool-d", round robin), living a
 *     uniform 0.5-5 s. Each burns THREAD_CPU_MS of its own CPU time at
 *     start (measured with CLOCK_THREAD_CPUTIME_ID), then sleeps out the
 *     rest of its life.
 *   cgroup churn: JOBS_PER_SEC new processes a second, each moved into its
 *     own cgroup v2 directory under /sys/fs/cgroup/loadgen with the cpu
 *     controller enabled, burning JOB_CPU_MS of CPU and living a uniform
 *     10-60 s. A reaper thread waits for each job and removes its cgroup.
 *
 * Runs for DURATION_S seconds, then waits for every job to exit.
 *
 * Usage (as root, for the cgroups):
 *   loadgen [duration_s=600] [thread_cpu_ms=10] [threads_per_sec=300]
 *           [jobs_per_sec=5] [job_cpu_ms=50]
 *
 * Build: cc -O2 -pthread -o loadgen loadgen.c
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define CG_ROOT "/sys/fs/cgroup"
#define CG_BASE CG_ROOT "/loadgen"
#define MAX_JOBS 65536

static long thread_cpu_ms = 10;
static long job_cpu_ms = 50;
static pid_t job_pids[MAX_JOBS];
static volatile int jobs_started = 0;
static volatile int jobs_reaped = 0;

static double now_s(clockid_t c) {
    struct timespec ts;
    clock_gettime(c, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* Burn `ms` of this thread's (or process's) CPU time. */
static void burn(clockid_t c, long ms) {
    double end = now_s(c) + ms / 1000.0;
    volatile unsigned long x = 0;
    while (now_s(c) < end)
        for (int i = 0; i < 1000; i++) x += i;
}

static void sleep_s(double s) {
    if (s <= 0) return;
    struct timespec ts = {(time_t)s, (long)((s - (time_t)s) * 1e9)};
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {}
}

static double uniform(unsigned *seed, double lo, double hi) {
    return lo + (hi - lo) * (rand_r(seed) / (double)RAND_MAX);
}

struct worker { int pool; unsigned seed; };

static void *worker_main(void *arg) {
    struct worker w = *(struct worker *)arg;
    free(arg);
    char name[16];
    snprintf(name, sizeof name, "pool-%c", 'a' + w.pool);
    pthread_setname_np(pthread_self(), name);
    double life = uniform(&w.seed, 0.5, 5.0);
    double start = now_s(CLOCK_MONOTONIC);
    burn(CLOCK_THREAD_CPUTIME_ID, thread_cpu_ms);
    sleep_s(life - (now_s(CLOCK_MONOTONIC) - start));
    return NULL;
}

static void write_file(const char *path, const char *s) {
    int fd = open(path, O_WRONLY);
    if (fd < 0 || write(fd, s, strlen(s)) < 0) {
        fprintf(stderr, "write %s: %s\n", path, strerror(errno));
        exit(1);
    }
    close(fd);
}

static void job_main(int n, unsigned seed) {
    char dir[128], procs[160], pid[32];
    snprintf(dir, sizeof dir, CG_BASE "/job-%d", n);
    if (mkdir(dir, 0755) != 0) _exit(2);
    snprintf(procs, sizeof procs, "%s/cgroup.procs", dir);
    snprintf(pid, sizeof pid, "%d", getpid());
    int fd = open(procs, O_WRONLY);
    if (fd < 0 || write(fd, pid, strlen(pid)) < 0) _exit(3);
    close(fd);
    double life = uniform(&seed, 10.0, 60.0);
    double start = now_s(CLOCK_MONOTONIC);
    burn(CLOCK_PROCESS_CPUTIME_ID, job_cpu_ms);
    sleep_s(life - (now_s(CLOCK_MONOTONIC) - start));
    _exit(0);
}

static void *reaper_main(void *arg) {
    (void)arg;
    for (;;) {
        int st;
        pid_t p = waitpid(-1, &st, 0);
        if (p < 0) {
            if (errno == ECHILD) { sleep_s(0.05); continue; }
            continue;
        }
        for (int i = 0; i < jobs_started; i++) {
            if (job_pids[i] == p) {
                char dir[128];
                snprintf(dir, sizeof dir, CG_BASE "/job-%d", i);
                if (rmdir(dir) != 0)
                    fprintf(stderr, "rmdir %s: %s\n", dir, strerror(errno));
                if (!WIFEXITED(st) || WEXITSTATUS(st) != 0)
                    fprintf(stderr, "job %d exited abnormally (%d)\n", i, st);
                break;
            }
        }
        __sync_fetch_and_add(&jobs_reaped, 1);
    }
    return NULL;
}

int main(int argc, char **argv) {
    long duration_s = argc > 1 ? atol(argv[1]) : 600;
    thread_cpu_ms = argc > 2 ? atol(argv[2]) : 10;
    long threads_per_sec = argc > 3 ? atol(argv[3]) : 300;
    long jobs_per_sec = argc > 4 ? atol(argv[4]) : 5;
    job_cpu_ms = argc > 5 ? atol(argv[5]) : 50;
    fprintf(stderr,
            "loadgen: %lds, threads %ld/s at %ldms CPU, jobs %ld/s at %ldms CPU\n",
            duration_s, threads_per_sec, thread_cpu_ms, jobs_per_sec, job_cpu_ms);

    /* cgroup v2: enable cpu for children of the root and of our base. */
    write_file(CG_ROOT "/cgroup.subtree_control", "+cpu");
    if (mkdir(CG_BASE, 0755) != 0 && errno != EEXIST) {
        perror("mkdir " CG_BASE);
        return 1;
    }
    write_file(CG_BASE "/cgroup.subtree_control", "+cpu");

    pthread_t reaper;
    pthread_create(&reaper, NULL, reaper_main, NULL);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&attr, 256 * 1024);

    /* One tick per thread spawn; jobs are spawned on the ticks that fall
     * due, so both rates hold on average over each second. */
    double t0 = now_s(CLOCK_MONOTONIC);
    double thread_step = 1.0 / threads_per_sec, job_step = 1.0 / jobs_per_sec;
    double next_thread = t0, next_job = t0;
    long threads = 0, failed = 0;
    unsigned seed = 12345;
    while (now_s(CLOCK_MONOTONIC) - t0 < duration_s) {
        double t = now_s(CLOCK_MONOTONIC);
        while (next_thread <= t) {
            struct worker *w = malloc(sizeof *w);
            w->pool = threads % 4;
            w->seed = seed + (unsigned)threads * 2654435761u;
            pthread_t th;
            if (pthread_create(&th, &attr, worker_main, w) != 0) { free(w); failed++; }
            threads++;
            next_thread += thread_step;
        }
        while (next_job <= t && jobs_started < MAX_JOBS) {
            int n = jobs_started;
            pid_t p = fork();
            if (p == 0) job_main(n, seed ^ (unsigned)n * 40503u);
            if (p > 0) { job_pids[n] = p; jobs_started = n + 1; }
            next_job += job_step;
        }
        double wake = next_thread < next_job ? next_thread : next_job;
        sleep_s(wake - now_s(CLOCK_MONOTONIC));
    }
    fprintf(stderr, "loadgen: spawned %ld threads (%ld failed), %d jobs; waiting for jobs\n",
            threads, failed, jobs_started);
    while (jobs_reaped < jobs_started) sleep_s(0.2);
    sleep_s(5.5); /* let the last threads finish their 5 s lives */
    rmdir(CG_BASE);
    fprintf(stderr, "loadgen: done\n");
    return 0;
}
