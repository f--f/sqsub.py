# sqsub.py
Python wrapper for SHARCNET's `sqsub` command, born out of my frustration during a bad period in my Master's in which nodes would randomly go offline without properly terminating my jobs. Can be used as a substitute for `sqsub`, with the additional features:

* Checks if job is frozen in run state (e.g. node crash) using log file.
* Sends e-mail message to user when job is finished / crashed.
* Allows default flags to sqsub to be set, but these can be overwritten
  by specifying them explicitly.

The daemon class was originally pulled from a simple tutorial ([here](http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/)), although unfortunately the original post seems to be inaccessible.
