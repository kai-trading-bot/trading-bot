import opsgenie_sdk
import os

from dotenv import load_dotenv
from typing import *

from src.utils.logger import logger

load_dotenv()


class OpsGenie:

    def __init__(self) -> None:
        self.conf = opsgenie_sdk.configuration.Configuration()
        self.conf.api_key['Authorization'] = os.getenv('OPSGENIE_API_KEY', '')
        self.api_client = opsgenie_sdk.api_client.ApiClient(configuration=self.conf)
        self.alert_api = opsgenie_sdk.AlertApi(api_client=self.api_client)

    def send(self,
             message: str,
             description: str = '',
             details: Optional[Dict] = None,
             priority: str = 'P3') -> None:
        payload = opsgenie_sdk.CreateAlertPayload(
            message=message,
            description=description,
            details=details or dict(),
            priority=priority,
        )
        try:
            self.alert_api.create_alert(create_alert_payload=payload)
        except opsgenie_sdk.ApiException as e:
            logger.error(f'Failed to send OpsGenie alert: {e}')
