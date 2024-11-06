from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import TaskInstance
import requests

class InformaticaLoginOperator(BaseOperator):
    @apply_defaults
    def __init__(self, username, password, **kwargs):
        super().__init__(**kwargs)
        self.username = username
        self.password = password

    def execute(self, context):
        login_url = "https://dm-us.informaticacloud.com/ma/api/v2/user/login"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        body = {"@type": "login", "username": self.username, "password": self.password}

        response = requests.post(login_url, json=body, headers=headers)
        response_data = response.json()

        self.log.info(f"Response Status Code: {response.status_code}")

        if response.status_code != 200:
            raise Exception(f"Login failed: {response_data}")

        server_url = response_data["serverUrl"]
        ic_session_id = response_data["icSessionId"]

        self.log.info(f"Login Successful!")

        # Store serverUrl and icSessionId in XCom
        context['ti'].xcom_push(key="serverUrl", value=server_url)
        context['ti'].xcom_push(key="icSessionId", value=ic_session_id)



