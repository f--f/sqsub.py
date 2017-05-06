"""Wrapper around default sqsub command on SHARCNET.

Features:
- Sends e-mail message when job is finished.
- Checks if job is frozen in run state (e.g. node crash) using log file
- Allows default arguments to sqsub to be set (for convenience).
"""

import os
import time
import sys
import subprocess
import string
from daemon import Daemon

# Modify these parameters accordingly
DEFAULT_SQSUB_ARGS = ['-q', 'chemeng', '-f', 'mpi', '-r', '14d']
TIME_DEAD = 60.*90.  # Max time (secs) between log updates, above which job is assumed to be frozen and is restarted automatically
POLL_INTERVAL_SEC = 15.  # Seconds between polling for changes in job state
DAEMON_PID_PATH = "/chemeng/{user}/.pid/".format(user=os.getenv("USER"))  # Directory to store PID information
EMAIL_COMMAND = r'cat %s | ssh -t orca "mail -s \"%s\" ${USER}@detritus.sharcnet.ca"'


class JobTracker(Daemon):
    """Daemon object which tracks the status of a single job.

    Attributes: 
        jobid (str): Job ID. 
        logfile (str): Path to log file for job.
    """

    def __init__(self, jobid, logfile, stdout):
        pidfile = os.path.join(DAEMON_PID_PATH, jobid)
        Daemon.__init__(self, pidfile, stdout=stdout, stderr=stdout)
        self.jobid = jobid
        self.logfile = logfile

    def job_state(self):
        """get job state (Q/R/D) from sqjobs"""
        sqjobs = subprocess.check_output(['sqjobs', '-l', self.jobid]).split()
        return sqjobs[sqjobs.index("state:")+1]

    def time_since_log_modified(self):
        """get time since log file was last modified in seconds"""
        return time.time() - os.path.getmtime(logfile)

    def offline_nodes(self, nodes_list):
        """Ping nodes. Returns None if OK; otherwise returns the first bad (offline) node."""
        for node in nodes_list:
            tmp = subprocess.check_output(["ping", "-c", "1", node])

    def out(self, message):
        print self.jobid + " | " + time.strftime("%b%d %H:%M:%S") + " :", message

    def run(self):
        """start the daemon"""
        # First wait for log file to be created / job to finish queueing
        while self.job_state() == "Q" or not os.path.isfile(self.logfile):
            self.out("Waiting for log file ({}) and/or job state ({})...".format(os.path.isfile(self.logfile), self.job_state()))
            time.sleep(POLL_INTERVAL_SEC)
        # Get list of nodes once job has started. If job is dead at this point, it will return empty list (not a big deal)
        nodes_list = subprocess.check_output("sqhosts | grep %s | awk '{print $1}'" % self.jobid, shell=True).split()
        self.out("Job started. Running on nodes: {}".format(nodes_list))
        # Once log file is created, continuously check whether job is frozen
        frozen_notification = False
        while True:
            self.out("Seconds since log was last modified: {}".format(self.time_since_log_modified()))
            # Check if job is dead
            if self.job_state() == "D":
                self.out("Job state is D (finished/dead), exiting...")
                os.system(EMAIL_COMMAND % (self.logfile, self.jobid + " | JOB ENDED | $(date)"))
                break
            # Ping nodes, check if down
            try:
                self.offline_nodes(nodes_list)
                self.out("Successfully pinged nodes: {}".format(nodes_list))
            except subprocess.CalledProcessError:
                self.out("A node seems to be offline, exiting...")
                os.system(EMAIL_COMMAND % (self.logfile, self.jobid + " | NODE FAILURE | $(date)"))
                break
            # If log file hasn't updated in a while, send e-mail
            if self.time_since_log_modified() > TIME_DEAD and not frozen_notification:
                self.out("Job seems to be frozen?")
                os.system(EMAIL_COMMAND % (self.logfile, self.jobid + " | FROZEN JOB ? | $(date)"))
                frozen_notification = True
                # Don't exit yet; keep going in case job continues (so another notification is sent on job death)
            time.sleep(POLL_INTERVAL_SEC)
        self.out("Stopping daemon for this job...")
        self.stop()
        sys.exit()  # This stops the daemon and deletes the pidfile


if __name__ == "__main__":

    # Attempt to submit job (concatenate arguments)
    args = ["sqsub"] + DEFAULT_SQSUB_ARGS + sys.argv[1:]
    print "Command:", string.join(args)
    try:
        output = subprocess.check_output(args)
    except subprocess.CalledProcessError:
        print "Job submission exited with non-zero status. Bad job submission?"
        sys.exit()

    jobid = output.split()[-1]
    print "Job submitted. Job ID:", jobid
    # Check if job submission was valid based on whether ID is a number
    if not jobid.isdigit():
        print "Invalid Job ID returned. Job may have not submitted successfully. Exiting..."
        sys.exit()

    # Use sqjobs to get absolute path of the log file
    sqjobs = subprocess.check_output(['sqjobs', '-l', jobid]).split()
    logfile = sqjobs[sqjobs.index("file:")+1]
    # If %J was used in the logfile, then replace it with the actual job id (from qstat)
    if "${PBS_JOBID}" in logfile:
        print "Replace job id in log file name..."
        qstat = subprocess.check_output(['qstat', '-f', jobid]).split()
        logfile = logfile.replace("${PBS_JOBID}", qstat[2])
    print "Log file:", logfile

    # Start daemon
    if not os.path.exists(DAEMON_PID_PATH):
        os.makedirs(DAEMON_PID_PATH)
    stdout = os.path.join(DAEMON_PID_PATH, "tracker_log.txt")
    print "Starting daemon. Output log is in {}".format(stdout)
    daemon = JobTracker(jobid, logfile, stdout)
    daemon.start()
