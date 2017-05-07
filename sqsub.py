#!/usr/bin/env python
__doc__ = """Wrapper around default sqsub command on SHARCNET.
Usage:

Features:
- Sends e-mail message when job is finished.
- Checks if job is frozen in run state (e.g. node crash) using log file
- Allows default arguments to sqsub to be set (for convenience).
"""

import os
import time
import sys
import subprocess
from daemon import Daemon

# Modify these parameters accordingly
DEFAULT_SQSUB_ARGS = ['-q', 'chemeng', '-f', 'mpi', '-r', '14d']
# A job is considered frozen if its log was last updated above this limit.
TIME_DEAD = 60. * 90.
POLL_INTERVAL_SEC = 15.  # Seconds between polling for changes in job state
# Directory to store PID information
DAEMON_PID_PATH = "/chemeng/{user}/.pid/".format(user=os.getenv("USER"))
EMAIL_COMMAND = r'cat %s | ' \
    'ssh orca "mail -s \"%s\" ${USER}@detritus.sharcnet.ca"'


def get_offline_nodes(nodes_list):
    """Ping list of nodes and returns offline nodes (from failed pings)."""
    offline_nodes = []
    for node in nodes_list:
        try:
            subprocess.check_output(["ping", "-c", "1", node])
        except subprocess.CalledProcessError:
            offline_nodes.append(node)
    return offline_nodes


def submit_job(args):
    """Submit a job and return job ID if successful, otherwise None.
    This is a wrapper around sqsub.

    Args:
        args: string containing arguments to sqsub.
    Returns:
        Returns the submitted job's ID. This corresponds to the final "word" in
        the output from sqsub after a successful submit.
        If submission was unsucessful, return"""

    args = ["sqsub"] + args
    try:
        output = subprocess.check_output(args)
        jobid = output.split()[-1]
    except subprocess.CalledProcessError:
        print "Job submission exited with non-zero status. Bad arguments?"
        return None

    return Job(jobid)


class Job():
    """Job class. Represents a single job.
    Queries job status from sqjobs once when created.

    Attributes:
        jobid (str): Job ID.
        logfile (str): Path to log file for job.
        nodes (list of int): List of nodes the job is running on.
        state (str): either 'Q' queued, 'R' running, or 'D' dead.
    """

    def __init__(self, jobid):
        self.id = jobid
        self.log = None
        self.state = None
        self.nodes = []

        self.refresh_log_path()
        self.refresh_job_state()
        self.refresh_nodes()

    def query(self, att):
        """Return output from sqjobs on this job for a single attribute."""
        stat = subprocess.check_output(['sqjobs', '-l', self.id]).split()
        return stat[stat.index(att) + 1]

    def refresh_log_path(self):
        logfile = self.query("file:")
        # If %J was used in the logfile, replace it with the actual job id
        if "${PBS_JOBID}" in logfile:
            print "Replace job id in log file name..."
            qstat = subprocess.check_output(['qstat', '-f', self.id]).split()
            logfile = logfile.replace("${PBS_JOBID}", qstat[2])
        self.log = logfile

    def refresh_job_state(self):
        self.state = self.query("state:")

    def refresh_nodes(self):
        self.nodes = subprocess.check_output(
            "sqhosts | grep %s | awk '{print $1}'" % self.id,
            shell=True).split()


class JobTracker(Daemon):
    """Daemon object which tracks the status of a single job.
    Relies on Job class to query job status.

    Attributes:
        job: Job object.
    """

    def __init__(self, job, stdout):
        pidfile = os.path.join(DAEMON_PID_PATH, job.id)
        Daemon.__init__(self, pidfile, stdout=stdout, stderr=stdout)
        self.job = job

    def time_since_log_modified(self):
        """get time since log file was last modified in seconds"""
        return time.time() - os.path.getmtime(self.job.log)

    def out(self, message):
        """write to stdout"""
        print self.job.id, "|", time.strftime("%b%d %H:%M:%S"), ":", message

    def email(self, message):
        """send e-mail with message in subject line"""
        os.system(EMAIL_COMMAND % (self.job.log, self.job.id + "|" + message))

    def run(self):
        """start the daemon"""

        # Wait for job to start running (state = R, generate log file)
        while not self.job.state == "R" or not os.path.isfile(self.job.log):
            self.out("Waiting for log file ({}) and/or job state ({})...".
                     format(os.path.isfile(self.job.log), self.job.state))
            self.job.refresh_job_state()
            self.job.refresh_log_path()
            time.sleep(POLL_INTERVAL_SEC)

        # Get list of nodes once job has started.
        time.sleep(POLL_INTERVAL_SEC)
        self.job.refresh_nodes()

        self.out("Job started. Running on nodes: {}".format(self.job.nodes))
        # Once log file is created, continuously check whether job is frozen
        frozen_note = False

        while True:
            self.out("Seconds since log was last modified: {}".format(
                self.time_since_log_modified()))
            self.job.refresh_job_state()

            # Check if job is dead
            if self.job.state == "D":
                self.out("Job state is D (finished/dead), exiting...")
                self.email("JOB ENDED")
                break

            # Ping nodes, check if a node is down
            if len(get_offline_nodes(self.job.nodes)) > 0:
                self.out("A node seems to be offline, exiting...")
                self.email("NODE FAILURE")
                break

            # If log file hasn't updated in a while, send e-mail
            if self.time_since_log_modified() > TIME_DEAD and not frozen_note:
                self.out("Job seems to be frozen?")
                self.email("FROZEN JOB?")
                frozen_note = True
                # Don't exit yet; keep going in case job continues
                # (so that another notification is sent on job death, etc.)

            time.sleep(POLL_INTERVAL_SEC)

        self.out("Stopping daemon for this job...")
        self.stop()
        self.delpid()


if __name__ == "__main__":

    print "Request: sqsub", DEFAULT_SQSUB_ARGS + sys.argv[1:]
    myjob = submit_job(DEFAULT_SQSUB_ARGS + sys.argv[1:])
    assert myjob is not None, "Job did not submit successfully - check args?"
    print "Job submitted. Job ID:", myjob.id

    # Start daemon
    if not os.path.exists(DAEMON_PID_PATH):
        os.makedirs(DAEMON_PID_PATH)
    stdout = os.path.join(DAEMON_PID_PATH, "tracker_log.txt")
    print "Starting daemon. Output log is in {}".format(stdout)
    daemon = JobTracker(myjob, stdout)
    daemon.start()
