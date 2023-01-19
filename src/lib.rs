use std::{time::{Instant}, sync::{Arc, Mutex, Condvar, MutexGuard}, thread::JoinHandle, collections::HashMap};

use schedule::Schedule;
use tracing::{info, warn, info_span, debug_span, debug};

/// Defines the [Schedule](crate::schedule::Schedule) trait and various implementations.
pub mod schedule {
    use std::{time::{Instant, Duration}};

    pub trait Schedule : Send + 'static {
        fn next_start_time(&mut self, latest_start_time: Instant, latest_end_time: Instant) -> Option<Instant>;
    }
    
    impl<T> Schedule for T where T : FnMut(Instant, Instant) -> Option<Instant> + Send + 'static {
        fn next_start_time(&mut self, latest_start_time: Instant, latest_end_time: Instant) -> Option<Instant> {
            self(latest_start_time, latest_end_time)
        }
    }
    
    /// Returns a [Schedule] which executes the job once.
    pub fn once() -> impl Schedule {
        let mut executed = false;
        move |_, _| {
            if executed {
                None
            } else {
                executed = true;
                Some(Instant::now())
            }
        }
    }

    /// Returns a [Schedule] which runs the job constantly, as fast as possible.
    pub fn always() -> impl Schedule {
        |_, _| Some(Instant::now())
    }

    /// Returns a [Schedule] which inserts a fixed delay between the end of one job
    /// execution and the start of the next. Note that this means that how often jobs
    /// execute depends on how long jobs take to run.
    pub fn fixed_delay(delay: Duration) -> impl Schedule {
        move |_, _| Some(Instant::now() + delay)
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
        Ok(move |_, _| {
            schedule.upcoming(chrono::Utc).next().and_then(|when| {
                let duration = when - chrono::Utc::now();
                Some(Instant::now() + duration.to_std().ok()?)
            })
        })
    }
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
/// as well as to get
pub struct JobRunner {
    join_on_drop: bool,
    jobs: HashMap<String, JobHandle>,
}

impl JobRunner {
    pub fn new() -> Self {
        Self {
            join_on_drop: false,
            jobs: HashMap::new(),
        }
    }

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

    pub fn statuses<'this>(&'this self) -> impl Iterator<Item = (&'this String, JobStatus)> + 'this {
        self.jobs.iter().flat_map(|(name, handle)| {
            let status = match handle.status.lock() {
                Ok(status) => status,
                Err(_) => return None
            };
            Some((name, status.clone()))
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn start(&mut self, job: Job) -> std::io::Result<()> {
        let status = Arc::new(Mutex::new(JobStatus::default()));
        let shutdown = Arc::new((Mutex::new(false), Condvar::new()));
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
        self.jobs.insert(job.name, JobHandle {
            status,
            shutdown,
            join_handle,
        });
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
            if let Ok(mut shutdown) = handle.shutdown.0.lock() {
                if !*shutdown {
                    info!("Signaled job to shut down");
                    *shutdown = true;
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
        self.join_all();
    }
}

struct JobHandle {
    status: Arc<Mutex<JobStatus>>,
    shutdown: Arc<(Mutex<bool>, Condvar)>,
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
    /// whenever the job is not running.
    pub current_start_time: Option<Instant>,
}

fn run_job(
    name: String,
    mut schedule: Box<dyn Schedule>,
    mut logic: Box<dyn FnMut()>,
    status: Arc<Mutex<JobStatus>>,
    shutdown: Arc<(Mutex<bool>, Condvar)>) {
    let _fn_span = tracing::info_span!("run_job", job = name);
    let mut latest_start_time = Instant::now();
    let mut latest_end_time = latest_start_time;
    loop {
        let schedule_result = {
            let _span = debug_span!("job_schedule");
            info!("Invoking job schedule");
            schedule.next_start_time(latest_start_time, latest_end_time)
        };
        let next_start_time = match schedule_result {
            Some(t) => t,
            None => {
                info!("Job exiting run loop because schedule returned None for next start time");
                break;
            }
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
        latest_start_time = {
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
            now
        };

        // Invoke the logic.
        {
            let _span = debug_span!("job_logic");
            info!("Invoking job logic");
            logic();
        }

        // Update the JobStatus for the end of the current run.
        latest_end_time = {
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
            now
        };
    }
}

fn sleep_until(target_time: Instant, shutdown: &Arc<(Mutex<bool>, Condvar)>) -> bool {
    let mut guard: MutexGuard<bool> = match shutdown.0.lock() {
        Ok(guard) => guard,
        Err(_) => {
            warn!("Sleep loop encountered poisoned shutdown mutex when acquiring initial shutdown signal lock, treating as shutdown signal");
            return true;
        }
    };
    loop {
        if *guard {
            info!("Sleep loop exiting due to shutdown signal being true");
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