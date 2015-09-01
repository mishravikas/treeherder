import logging
import os
import yaml
import jsonschema
import time
import datetime
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
    status_rank = {
        "unscheduled": 0,
        "pending": 1,
        "running": 2,
        "success": 3,
        "fail": 3,
        "exception": 3,
        "canceled": 3
    }
    completed_status_rank = 3
    test_result_map = {
        "success": "success",
        "fail": "testfail",
        "exception": "exception",
        "canceled": "usercancel"
    }
    build_result_map = {
        "success": "success",
        "fail": "busted",
        "exception": "exception",
        "canceled": "usercancel"
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
                for pulse_job in job_list:
                    if (pulse_job['status'] != "unscheduled"):
                        try:
                            storeable_job_list.append({
                                "revision_hash": rs_lookup[pulse_job['origin']['revision']],
                                "job": {
                                    "job_guid": pulse_job['jobGuid'],
                                    "name": pulse_job['display'].get('jobName', ''),
                                    "job_symbol": pulse_job['display'].get('jobSymbol', ''),
                                    "group_name": pulse_job['display'].get('groupName', ''),
                                    "group_symbol": pulse_job['display'].get('groupSymbol', ''),
                                    "product_name": pulse_job.get("productName"),
                                    "state": self.get_state(pulse_job),
                                    "result": self.get_result(pulse_job),
                                    "reason": pulse_job["reason"],
                                    "who": pulse_job["who"],
                                    "submit_timestamp": self.to_timestamp(pulse_job['times']['scheduled']),
                                    "start_timestamp": self.to_timestamp(pulse_job['times']['started']),
                                    "end_timestamp": self.to_timestamp(pulse_job['times']['completed']),
                                    "machine": self.get_machine(pulse_job),
                                    "build_platform": self.get_platform(pulse_job['machine'].get("build", None)),
                                    "machine_platform": self.get_platform(pulse_job['machine'].get("run", None)),
                                    "option_collection": self.get_option_collection(pulse_job),
                                    "log_references": pulse_job['log_references'],
                                    "artifacts": pulse_job['artifacts'],
                                },
                                "coalesced": []
                            })
                        except AttributeError as e:
                            logger.warn("Skipping job due to bad attribute", exc_info=1)


    def transform(self, pulse_job):
        """
        Transform a pulse job into a job that can be written to disk.

        Also generate log references and artifacts
        """
        job_placeholders = []

    def get_option_collection(self, job):
        option_collection = {["opt", True]}
        if "optionCollection" in job:
            option_collection = {}
            for option in job["optionCollection"]:
                option_collection[option] = True
        return option_collection

    def get_platform(self, platform_src):
        platform = None
        if platform_src:
            platform = {
                "platform": platform_src['platform'],
                "os_name": platform_src['osName'],
                "architecture": platform_src['architecture']
            }
        return platform

    def get_machine(self, job):
        machine = "unknown"
        if "build" in job["machine"]:
            machine = job['machine']['build']['machineName']
        if "run" in job["machine"]:
            machine = job['machine']['run']['machineName']
        return machine

    def get_state(self, job):
        status = job["status"]
        state = "completed"
        if status in ["pending", "running"]:
            state = status
        elif status == "unscheduled":
            raise AttributeError("unscheduled not a supported status at this time.")
        return state

    def get_result(self, job):
        result = "unknown"
        status = job['status']
        if self.status_order[status] >= self.completed_status_rank:
            if job['jobKind'] == "build":
                result = self.build_result_map[status]
            else:
                result = self.test_result_map[status]

        return result

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

    def to_timestamp(datestr):
        """get a timestamp from a datestr like 2014-03-31"""
        return time.mktime(datetime.datetime.strptime(
            datestr
        ).timetuple())
