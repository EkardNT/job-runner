//! A simple [JobRunner] which gives each job a dedicated thread and allows for 
//! configurable delays between each invocation of the job's logic.
//! 
//! # Example
//! 
//! A program using the [JobRunner] is expected to have this basic outline:
//! 
//! ```rust
//! use job_runner::{Job, JobRunner, fixed_delay};
//! 
//! fn main() {
//!     // At program startup, create the JobRunner and register your tasks.
//!     let mut job_runner = JobRunner::new();
//!     job_runner.start(Job::new(
//!         "cool_job",
//!         fixed_delay(std::time::Duration::from_secs(5)),
//!         my_cool_job));
//! 
//!     // Do other things in your program...
//! 
//!     // Then, when shutting down your program, signal all the job threads
//!     // to stop running.
//!     job_runner.stop_all();
//! 
//!     // Maybe signal other parts of your program to gracefully shut down too...
//! 
//!     // Finally (and optionally) wait for the job threads to actually exit.
//!     job_runner.join_all();
//! }
//! 
//! fn my_cool_job() {
//!     // Do cool things..
//! }
//! ```

#![deny(rustdoc::broken_intra_doc_links)]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::{time::{Instant, Duration}, sync::{Arc, Mutex, Condvar}, thread::JoinHandle, collections::HashMap};

use tracing::{info, warn, info_span, debug_span, debug};

/// A [Schedule] implementation controls when jobs are executed. All that the [JobRunner]
/// does is invoke a job in an infinite loop (until the [JobRunner] is shut down), with
/// a delay between runs. The delay is controlled by the [Schedule], and schedules can specify
/// either fixed or varying delays.
pub trait Schedule : Send + 'static {
    /// Returns when the next job execution should occur at. Typical implementations of this
    /// method will choose the next delay by looking at the current time using mechanisms such
    /// as [Instant::now()](std::time::Instant::now).
    fn next_start_delay(&mut self) -> Duration;
}

impl<T> Schedule for T where T : FnMut() -> Duration + Send + 'static {
    fn next_start_delay(&mut self) -> Duration {
        self()
    }
}

/// Returns a [Schedule] which runs the job constantly, as fast as possible.
pub fn spin() -> impl Schedule {
    || Duration::ZERO
}

/// Returns a [Schedule] which inserts a fixed delay between the end of one job
/// execution and the start of the next. Note that this means that how often jobs
/// execute depends on how long jobs take to run.
pub fn fixed_delay(delay: Duration) -> impl Schedule {
    move || delay
}

/// Returns a [Schedule] which runs jobs on a cron schedule. If a job execution
/// runs overlong, then the executions which were overlapped will simply be skipped.
/// For example, if a job is scheduled to run every second, but takes 5 seconds to run,
/// then the 4 executions that should have happened while the slow job was executing
/// will be skipped - only every 5th scheduled job will actually execute.
#[cfg(feature = "cron")]
pub fn cron(schedule: &str) -> Result<impl Schedule, ::cron::error::Error> {
    use std::str::FromStr;
    let schedule = ::cron::Schedule::from_str(schedule)?;
    Ok(move || {
        schedule.upcoming(chrono::Utc).next().and_then(|when| {
            when.signed_duration_since(chrono::Utc::now()).to_std().ok()
        }).unwrap_or(Duration::ZERO)
    })
}

/// A description of a job that can be registered with a [JobRunner].
pub struct Job {
    name: String,
    schedule: Box<dyn Schedule + Send + 'static>,
    logic: Box<dyn FnMut() + Send + 'static>,
    thread_builder: Option<Box<dyn FnOnce() -> std::thread::Builder>>,
}

impl Job {
    /// Construct a new [Job] with a name, schedule, and the actual job logic.
    pub fn new(name: impl Into<String>, schedule: impl Schedule, logic: impl FnMut() + Send + 'static) -> Self {
        Self {
            name: name.into(),
            schedule: Box::new(schedule),
            logic: Box::new(logic),
            thread_builder: None,
        }
    }

    /// Optional setting which allows you to customize the thread on which this job will
    /// be executed. If this function is not called, the default thread builder sets the
    /// thread name to the name of the job and does not specify an explicit stack size.
    pub fn thread_builder(self, thread_builder: impl FnOnce() -> std::thread::Builder + 'static) -> Self {
        Self {
            thread_builder: Some(Box::new(thread_builder)),
            ..self
        }
    }
}

/// The main coordinator for running jobs. It exposes methods to start and stop jobs,
/// as well as to get the status of a job or all jobs.
/// 
/// Each job added to the [JobRunner] is given a dedicated thread to execute on, therefore
/// the number of threads created by the [JobRunner] is equal to the number of jobs
/// which are [started](JobRunner::start).
pub struct JobRunner {
    join_on_drop: bool,
    jobs: HashMap<String, JobHandle>,
}

impl JobRunner {
    /// Initialize a new [JobRunner] with no jobs started yet.
    pub fn new() -> Self {
        Self {
            join_on_drop: false,
            jobs: HashMap::new(),
        }
    }

    /// Allows you to configure the [JobRunner] to wait for job threads to exit
    /// when [dropped](Drop::drop). The default value for this option is `false`,
    /// which is equivalent to calling the [stop_all](JobRunner::stop_all) method
    /// at drop time. Passing `true` for this option is equivalent to calling the
    /// [join_all](JobRunner::join_all) method at drop time.
    pub fn join_on_drop(&mut self, join_on_drop: bool) -> &mut Self {
        self.join_on_drop = join_on_drop;
        self
    }

    /// Gets the latest status of a specific job by the job's name.
    pub fn status(&self, job_name: &str) -> Option<JobStatus> {
        self.jobs.get(job_name).and_then(|handle| {
            Some(handle.status.lock().ok()?.clone())
        })
    }

    /// Gets an iterator over all job statuses. The iterator item tuple's first entry
    /// is the name of the job.
    pub fn statuses(&self) -> impl Iterator<Item = (&String, JobStatus)> {
        self.jobs.iter().flat_map(|(name, handle)| {
            let status = match handle.status.lock() {
                Ok(status) => status,
                Err(_) => return None
            };
            Some((name, status.clone()))
        })
    }

    /// Registers a job and starts it executing on a dedicated thread. The job schedule's
    /// [Schedule::next_start_delay] method will be called to determine when the
    /// first job execution should occur.
    #[tracing::instrument(skip_all)]
    pub fn start(&mut self, job: Job) -> std::io::Result<()> {
        let status = Arc::new(Mutex::new(JobStatus::default()));
        let shutdown = Arc::new((Mutex::new((false, 0)), Condvar::new()));
        let thread_builder = match job.thread_builder {
            Some(thread_builder) => thread_builder(),
            None => std::thread::Builder::new()
                .name(job.name.clone())
        };
        let join_handle = thread_builder.spawn({
            let status = Arc::clone(&status);
            let shutdown = Arc::clone(&shutdown);
            let name = job.name.clone();
            move || {
                run_job(name, job.schedule, job.logic, status, shutdown)
            }
        })?;
        let prev_handle = self.jobs.insert(job.name, JobHandle {
            status,
            shutdown,
            join_handle,
        });
        if let Some(handle) = prev_handle {
            handle.shutdown.0.lock().unwrap().0 = true;
            let _ = handle.join_handle.join();
        }
        Ok(())
    }

    /// Signal to all jobs to stop executing. This will prevent any further job runs from
    /// starting, but will not preemptively interrupt any currently-executing job runs.
    /// Although the [join_all](JobRunner::join_all) method also signals all jobs to stop
    /// executing, this method can still be useful to call at the start of application shut
    /// down, if you have other parts of the program that you want to begin shutting down
    /// too before calling the blocking [join_all](JobRunner::join_all) method. This method
    /// signals, but does not block.
    #[tracing::instrument(skip_all)]
    pub fn stop_all(&mut self) {
        info!("Signaling {} jobs to stop", self.jobs.len());
        for (name, handle) in &mut self.jobs {
            let _span = info_span!("stop_job", job = name);
            if let Ok(mut guard) = handle.shutdown.0.lock() {
                if !guard.0 {
                    info!("Signaled job to shut down");
                    guard.0 = true;
                }
            } else {
                warn!("Received poison error when trying to acquire shutdown signal lock");
            }
        }
    }

    /// Signal to all jobs to stop executing and then waits for the job threads to
    /// exit before returning. Jobs that are waiting for the next scheduled run will
    /// exit immediately, but currently-executing jobs will be allowed to complete
    /// their current run - the [JobRunner] does not itself define any mechanism for
    /// preemptively interrupting running jobs. That means that how long this method
    /// takes to execute depends on how long the slowest currently-running job takes
    /// to finish its run. If you have particularly long-running jobs, you may want
    /// to pass them a separate cancellation token that you call before invoking this
    /// method.
    #[tracing::instrument(skip_all)]
    pub fn join_all(&mut self) {
        self.stop_all();
        info!("Joining {} jobs", self.jobs.len());
        for (name, handle) in self.jobs.drain() {
            let _span = info_span!("join_job", job = name);
            match handle.join_handle.join() {
                Ok(()) => {
                    info!("Job thread exited normally");
                },
                Err(_) => {
                    warn!("Job thread exited with a panic");
                }
            }
        }
    }
}

impl Drop for JobRunner {
    fn drop(&mut self) {
        if self.join_on_drop {
            self.join_all();
        } else {
            self.stop_all();
        }
    }
}

struct JobHandle {
    status: Arc<Mutex<JobStatus>>,
    shutdown: Arc<(Mutex<(bool, usize)>, Condvar)>,
    join_handle: JoinHandle<()>,
}

/// A snapshot of the current status of a job.
#[derive(Default, Debug, Clone)]
pub struct JobStatus {
    /// How many times has the job been executed. This value is incremented when the
    /// job logic execution begins, not when it ends.
    pub runs: usize,
    /// Whether the job logic is currently executing (true) or the job is sleeping until
    /// the next scheduled run (false).
    pub running: bool,
    /// The time at which the latest finished job run started. May be [None] if the job
    /// has never executed yet.
    pub latest_start_time: Option<Instant>,
    /// The time at which the latest finished job run ended. May be [None] if the job
    /// has never executed yet, or has not finished executing for the first time.
    pub latest_end_time: Option<Instant>,
    /// The time at which the currently running execution started. This will be [None]
    /// whenever the job is not executing.
    pub current_start_time: Option<Instant>,
    /// When the next job execution is scheduled for. This will be [None] when the job
    /// is executing.
    pub next_start_time: Option<Instant>,
}

fn run_job(
        name: String,
        mut schedule: Box<dyn Schedule>,
        mut logic: Box<dyn FnMut()>,
        status: Arc<Mutex<JobStatus>>,
        shutdown: Arc<(Mutex<(bool, usize)>, Condvar)>) {
    let _fn_span = tracing::info_span!("run_job", job = name);
    loop {
        let next_start_time = {
            let _span = debug_span!("job_schedule");
            info!("Invoking job schedule");
            Instant::now() + schedule.next_start_delay()
        };

        // Update the JobStatus for the next start time.
        {
            let _span = debug_span!("job_next_start_status_update");
            debug!("Updating job status for next start time schedule");
            let mut status = match status.lock() {
                Ok(status) => status,
                Err(_) => {
                    warn!("Job exiting run loop due to poison error when locking status for next start time update");
                    break;
                }
            };
            status.next_start_time = Some(next_start_time);
        };
        
        let is_shutdown = {
            let _span = debug_span!("job_sleep");
            sleep_until(next_start_time, &shutdown)
        };
        if is_shutdown {
            info!("Job exiting run loop due to shutdown signal");
            break;
        }
        
        // Update the JobStatus for the start of the current run.
        let latest_start_time = {
            let _span = debug_span!("job_start_status_update");
            debug!("Updating job status for start of current run");
            let now = Instant::now();
            let mut status = match status.lock() {
                Ok(status) => status,
                Err(_) => {
                    warn!("Job exiting run loop due to poison error when locking status for start of job execution");
                    break;
                }
            };
            status.runs += 1;
            status.running = true;
            status.current_start_time = Some(now);
            status.next_start_time = None;
            now
        };

        // Invoke the logic.
        {
            let _span = debug_span!("job_logic");
            info!("Invoking job logic");
            logic();
        }

        // Update the JobStatus for the end of the current run.
        {
            let _span = debug_span!("job_end_status_update");
            debug!("Updating job status for end of current run");
            let now = Instant::now();
            let mut status = match status.lock() {
                Ok(status) => status,
                Err(_) => {
                    warn!("Job exiting run loop due to poison error when locking status for end of job execution");
                    break;
                }
            };
            status.running = false;
            status.current_start_time = None;
            status.latest_start_time = Some(latest_start_time);
            status.latest_end_time = Some(now);
        };
    }
}

fn sleep_until(target_time: Instant, shutdown: &Arc<(Mutex<(bool, usize)>, Condvar)>) -> bool {
    let mut guard = match shutdown.0.lock() {
        Ok(guard) => guard,
        Err(_) => {
            warn!("Sleep loop encountered poisoned shutdown mutex when acquiring initial shutdown signal lock, treating as shutdown signal");
            return true;
        }
    };
    loop {
        let (is_shutdown, execute_requests) = *guard;
        if is_shutdown || execute_requests > 0 {
            info!("Sleep loop exiting due to shutdown signal being true or execute requests present");
            return true;
        }
        let time_to_wait = Instant::now().saturating_duration_since(target_time);
        if time_to_wait.is_zero() {
            info!("Sleep loop finished waiting for time to pass");
            return false;
        }
        match shutdown.1.wait_timeout(guard, time_to_wait) {
            Ok((g, _)) => {
                guard = g
            },
            Err(_) => {
                warn!("Sleep loop saw poisoned shutdown mutex while sleeping, treating as shutdown signal");
                return true;
            }
        }
    }
}