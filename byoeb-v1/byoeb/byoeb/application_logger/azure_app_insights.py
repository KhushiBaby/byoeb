
import os
import json
from azure.monitor.opentelemetry import configure_azure_monitor
from typing import Any, Dict
from azure.monitor.events.extension import track_event

class AzureAppInsightsLogger:
    def __init__(
        self,
        logger_name,
        connection_string
    ):

        configure_azure_monitor(
            logger_name=logger_name,
            connection_string=connection_string,
        )

    def add_log(self,
        event_name,
        **kwargs
    ):
        if 'details' in kwargs:
            kwargs['details'] = json.dumps(kwargs['details'])
        track_event(event_name, kwargs)
        return