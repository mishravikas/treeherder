import json
import os

from treeherder.etl import pulse_consumer


def test_validate_jobs(sample_data):
    """
    Ensure the pending job with the missing resultset is queued for refetching
    """
    jpc = pulse_consumer.PulseConsumer()

    jpc.load_jobs(sample_data.pulse_jobs)

