import unittest
import sqsub

OFFLINE_HOSTS = ['tem100']  # Known offline host
INVALID_JOB = ['sleep', '0.1']  # Invalid job command (requires -o)
VALID_JOB = ['-o', '/tmp/test.log', '-r', '1h', 'sleep', '0.1']  # Valid job


class TestJobTracker(unittest.TestCase):

    def test_submit_invalid_job(self):
        """Test job submission with invalid arguments."""
        self.assertEqual(sqsub.submit_job(INVALID_JOB), None)

    def test_jobid_valid(self):
        """Test if code to get job ID after submission works by checking
        if ID is a numeral (as expected)."""
        job = sqsub.submit_job(VALID_JOB)
        self.assertTrue(job.id.isdigit())

    def test_offline_nodes(self):
        """Test offline node checking with known offline node."""
        self.assertEqual(sqsub.get_offline_nodes(OFFLINE_HOSTS), OFFLINE_HOSTS)


if __name__ == '__main__':
    unittest.main()
