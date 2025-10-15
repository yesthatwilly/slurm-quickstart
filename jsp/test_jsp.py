#!/usr/bin/python3
# Author: Willy Dizon <wdizon@asu.edu>
# This script is to help use test-driven development to implement
# business logic in the slurm job_submit.lua plugin.

import re
import shlex
import subprocess
import unittest
from functools import wraps
from os import getenv

import requests

# ------------------ User-configurable variables (edit here) ------------------
SLURM_CONTROLLER = "http://localhost:6820"
API_VERSION = "v0.0.40"
TEMPORARY_ADDITIONS = " -A root "          # extra sbatch params added during tests
SBATCH_BEGIN = '--begin="now+1second"'             # when to begin job
SBATCH_WRAP = '--wrap="sleep 1"'                   # wrapper for sbatch
SCONTROL_CMD = ["scontrol", "token"]               # command used to fetch token
# ---------------------------------------------------------------------------

class SlurmSubmissionError(Exception):
    def __init__(self, message, full_output):
        super().__init__(message)
        self.output = full_output

def get_token():
    """Run scontrol token and return the token (text after '=')."""
    result = subprocess.run(SCONTROL_CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with return code {result.returncode}: {result.stderr}")
    output = result.stdout.strip()
    if '=' in output:
        return output.split('=', 1)[1]
    raise ValueError("Unexpected output format")

def probe_restapi(resource, endpoint):
    """Call SLURM REST API and return (json, status_code)."""
    url = f"{SLURM_CONTROLLER}/{resource}/{API_VERSION}/{endpoint}"
    headers = {"X-SLURM-USER-TOKEN": token}
    response = requests.get(url, headers=headers)
    return response.json(), response.status_code

def query_slurm(query_type, value):
    if query_type == "jobid":
        response, status_code = probe_restapi('slurm', f"job/{value}")
        try:
            return response['jobs'][0]
        except IndexError:
            raise Exception(response['errors'][0]['description'])
    elif query_type == "userqos":
        response, status_code = probe_restapi('slurmdb', f"associations?user={value}")
        return (response['associations'][0]['qos'], response['associations'][0]['default']['qos'])

def run(params):
    """Convenience function to submit a job and return its job-info response."""
    def run_sbatch(params_list):
        try:
            proc = subprocess.Popen(shlex.split(params_list), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                return None, stderr
            return stdout, stderr
        except Exception as e:
            return None, str(e)

    def parse_output(stdout):
        job_id_pattern = re.compile(r'Submitted batch job (\d+)')
        for line in stdout.splitlines():
            m = job_id_pattern.search(line)
            if m:
                return int(m.group(1))
        return stdout.splitlines()

    sbatch_params = f"sbatch {SBATCH_BEGIN} {SBATCH_WRAP} {TEMPORARY_ADDITIONS}{params}"
    stdout, stderr = run_sbatch(sbatch_params)

    if stdout:
        jobid = parse_output(stdout)
        response = query_slurm('jobid', jobid)
        return response
    else:
        parsed = parse_output(stderr)
        raise SlurmSubmissionError(parsed[-1], parsed)

# ------------------ Unit-test decorators (cleaned & active) ------------------

def common_slurm_checks(func):
    """Decorator to perform common SLURM assertions after a test runs.

    Expects the decorated test to populate `self.details` with the job dictionary.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        func(self, *args, **kwargs)  # run the test (should set self.details)

        # Basic CPU assertions (mirror original behavior)
        self.assertEqual(self.details['cpus']['set'], True)
        self.assertEqual(self.details['cpus']['infinite'], False)

        # Time-limit assertions
        self.assertEqual(self.details['time_limit']['set'], True)
        self.assertEqual(self.details['time_limit']['infinite'], False)

        # Mail user asserted to be "<USER>@example.edu"
        # self.assertEqual(self.details['mail_user'], f"{getenv('USER')}@example.edu")

    return wrapper

def expect_equal(assertions_dict):
    """Decorator factory that checks a dictionary of expected values against self.details.

    Rules preserved from original:
    - If self.details[key] is a dict, compare against self.details[key]['number'].
    - If expected value is a tuple, ensure the value string in details splits into the same count
      and contains each expected member (subTest used for clarity).
    - Otherwise, assert key exists and equals expected value.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)  # run the test (should set self.details)

            for key, expected_value in assertions_dict.items():
                # If the stored field is a dict, compare against its 'number' entry.
                if isinstance(self.details.get(key), dict):
                    self.assertEqual(expected_value, self.details[key]['number'])
                    continue

                # If expected is a tuple -> treat as multiple expected tokens in a comma-separated string.
                if isinstance(expected_value, tuple):
                    with self.subTest(key=key):
                        # Ensure key exists
                        self.assertIn(key, self.details)

                        actual_vals = self.details[key].split(',')
                        # Ensure counts match
                        self.assertEqual(len(expected_value), len(actual_vals),
                                         f"Expected {len(expected_value)} values for key '{key}', "
                                         f"but got {len(actual_vals)}")
                        # Ensure each expected member is present
                        for value in expected_value:
                            with self.subTest(value=value):
                                self.assertIn(value, self.details[key])
                    continue

                # Default: ensure key exists and value equals expected_value
                self.assertIn(key, self.details)
                self.assertEqual(self.details[key], expected_value)

        return wrapper
    return decorator

def raise_with_message(exception_type, expected_message):
    """Decorator that asserts a test raises `exception_type` with `expected_message`."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            with self.assertRaises(exception_type) as cm:
                func(self, *args, **kwargs)
            actual_message = str(cm.exception)
            self.assertEqual(
                actual_message, expected_message,
                f"Expected {exception_type.__name__} message '{expected_message}', got '{actual_message}'"
            )
        return wrapper
    return decorator

# ---------------------------------------------------------------------------

# --------------------------- Unit tests ------------------------------------

class TestSlurm(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    # Basic test that checks a job is queued properly
    @raise_with_message(SlurmSubmissionError, "sbatch: error: Batch job submission failed: No partition specified or system default partition")
    def test_sbatch_blank(self):
        self.details = run('')

    # Basic test that checks a job is queued properly with basic conditions met
    @common_slurm_checks
    def test_sbatch_with_common_checks(self):
        self.details = run('-p general')

# --------------------------- Entry point -----------------------------------

if __name__ == '__main__':
    if getenv('SLURM_JWT'):
        print(f"Using environment-based token $SLURM_JWT")
        token = getenv('SLURM_JWT')
    else:
        token = get_token()

    unittest.main()
