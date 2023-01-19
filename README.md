A simple job runner library which gives each job a dedicated thread and allows for 
configurable delays between each invocation of the job's logic.

# Example

A program using the JobRunner utility is expected to have this basic outline:

```rust
use job_runner::{Job, JobRunner, fixed_delay};

fn main() {
    // At program startup, create the JobRunner and register your tasks.
    let mut job_runner = JobRunner::new();
    job_runner.start(Job::new(
        "cool_job",
        fixed_delay(std::time::Duration::from_secs(5)),
        my_cool_job));

    // Do other things in your program...

    // Then, when shutting down your program, signal all the job threads
    // to stop running.
    job_runner.stop_all();

    // Maybe signal other parts of your program to gracefully shut down too...

    // Finally (and optionally) wait for the job threads to actually exit.
    job_runner.join_all();
}

fn my_cool_job() {
    // Do cool things..
}
```