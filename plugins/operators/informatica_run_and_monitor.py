from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import time


class InformaticaRunAndMonitorJobOperator(BaseOperator):
    @apply_defaults
    def __init__(self, informatica_task_id, informatica_task_type, poll_interval=30, **kwargs):
        """
        Operator to start an Informatica job and monitor its status until completion.

        :param informatica_task_id: ID of the task to run
        :param informatica_task_type: Type of Informatica task
        :param poll_interval: Time (in seconds) between each status check
        """
        super().__init__(**kwargs)
        self.informatica_task_id = informatica_task_id
        self.informatica_task_type = informatica_task_type
        self.poll_interval = poll_interval

    def execute(self, context):
        # Retrieve session info from XCom
        server_url = context['ti'].xcom_pull(key="serverUrl")
        ic_session_id = context['ti'].xcom_pull(key="icSessionId")


        # Start the Job
        job_url = f"{server_url}/api/v2/job"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "icSessionId": ic_session_id
        }
        body = {"@type": "job", "taskId": self.informatica_task_id, "taskType": self.informatica_task_type}

        response = requests.post(job_url, json=body, headers=headers)
        response_data = response.json()

        if response.status_code != 200:
            raise Exception(f"Failed to start job: {response_data}")

        run_id = response_data["runId"]
        context['ti'].xcom_push(key="runId", value=run_id)
        self.log.info(f"Job started with runId: {run_id}")

        # Polling for Job Status
        status_url = f"{server_url}/api/v2/activity/activityLog?runId={run_id}"
        while True:
            response = requests.get(status_url, headers=headers)
            response_data = response.json()

            if response.status_code != 200:
                raise Exception(f"Failed to get job status: {response_data}")

            # Check if the response is a list and handle accordingly
            if isinstance(response_data, list) and len(response_data) > 0:
                status_data = response_data[0]  # Assuming the status is in the first entry
            elif isinstance(response_data, dict):
                status_data = response_data
            else:
                status_data = response_data
                #raise Exception(f"Unexpected response format: {response_data}")
                #raise Exception(f"Unexpected response format: {response_data}")

            # Check for endTime and errorMsg
            if isinstance(status_data, dict):
                error_msg = status_data.get("errorMsg", "N/A")
                state = status_data.get("state", 0)
            else:
                error_msg = "N/A"
                state = 0

            if state == 3:
                self.log.info(f"Job failed with error: {error_msg}")
                raise Exception(f"Job failed with error: {error_msg}")
            elif state == 1:
                self.log.info("Job completed successfully.")
                #self.log.info({end_time})
                time.sleep(60)
                break
            else:
                self.log.info(f"Job still running. Checking again in {self.poll_interval} seconds...")
                time.sleep(self.poll_interval)
