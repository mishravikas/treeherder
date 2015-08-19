import logging
import os
import yaml
import jsonschema
from collections import defaultdict

from treeherder.model.derived.jobs import JobsModel

logger = logging.getLogger(__name__)

######
# This will circumvent the store_job_data method in jobs.  That method has to
# do different work from what this does.  So this will do the transformation
# and will call the specific jobsModel functions to write the data
#
# Not clear if I would create the placeholders here, or create objects that
# the jobsModel will use to create placeholders.
#
######
class PulseConsumer:
    jobs_schema = None
    artifact_schema = None

    # status can only transition from lower to higher order.  If a job comes
    # in to update a status to a lower order, it will be skipped as out of
    # sequence.
    status_order = {
        "unscheduled": 0,
        "pending": 1,
        "running": 2,
        "success": 3,
        "fail": 4,
        "exception": 5,
        "canceled": 6
    }

    @staticmethod
    def get_jobs_schema():
        with open("{0}/job.yml".format(
                  os.path.dirname(__file__))) as f:
            return yaml.load(f)

    def __init__(self):
        self.jobs_schema = self.get_jobs_schema()

    def process_job_list(self, all_jobs_list):
        validated_jobs = self._get_validated_jobs_by_project(all_jobs_list)

        for project, job_list in validated_jobs.items():
            with JobsModel(project) as job_model:
                rs_lookup = job_model.get_revision_resultset_lookup(
                    [x['origin']['revision'] for x in job_list])
                storeable_job_list = []
                for job in job_list:
                    storeable_job_list.append({
                        "revision_hash": rs_lookup[job['origin']['revision']],
                        "job": {
                            "job_guid": job['jobGuid'],
                            "name": job['display'].get('jobName', ''),
                            "job_symbol": job['display'].get('jobSymbol', ''),
                            "group_name": job['display'].get('groupName', ''),
                            "group_symbol": job['display'].get('groupSymbol', ''),


                            "product_name": job.get("productName"),
                            "state": self.getState(job),
                            "result": self.getResult(job),
                            "reason": "scheduler",
                            "who": "sendchange-unittest",
                            "submit_timestamp": 1365732271,
                            "start_timestamp": "20130411165317",
                            "end_timestamp": "1365733932",
                            "machine": "tst-linux64-ec2-314",
                            "build_url": "http://....",
                            "build_platform": {
                                "platform": "Ubuntu VM 12.04",
                                "os_name": "linux",
                                "architecture": "x86_64",
                                "vm": "true"
                            },
                            "machine_platform": {
                                "platform": "Ubuntu VM 12.04",
                                "os_name": "linux",
                                "architecture": "x86_64",
                                "vm": true
                            },
                            "option_collection": {
                                "opt": true
                            },
                            "log_references": job['log_references'],
                            "artifacts": job['artifacts'],
                        },
                        "coalesced": []
                    })


    def transform(self, pulse_job):
        """
        Transform a pulse job into a job that can be written to disk.

        Also generate log references and artifacts
        """
        job_placeholders = []







    def _get_validated_jobs_by_project(self, jobs_list):
        validated_jobs = defaultdict(list)
        for pulse_job in jobs_list:
            try:
                jsonschema.validate(pulse_job, self.jobs_schema)
                validated_jobs[pulse_job['project']].append(pulse_job)
            except (jsonschema.ValidationError,jsonschema.SchemaError) as e:
                logger.error("JSON Schema validation error during job ingestion: {}".format(e))

        return validated_jobs

    def load(self, jobs):
        pass

    def _update_coalesced_jobs(self, job_guid_list):
        pass

    def _update_retried_job(self, job_guid):
        pass

    def _load_log_references(self, log_references):
        pass

    def _load_artifacts(self, artifact_list):
        pass

