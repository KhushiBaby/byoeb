
import os
import json
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from azure.monitor.events.extension import track_event

class AppLogger:
    def __init__(self):
        # return

        configure_azure_monitor(
            logger_name=os.environ["APPINSIGHT_LOGGER"],
            connection_string=os.environ["APPINSIGHT_CONNECTION_STRING"]
        )

        # trace.set_tracer_provider(TracerProvider())
        # self.tracer = trace.get_tracer(__name__)

    def add_event(self,
        event_name,
        event_properties):

        track_event(event_name, event_properties)
        return

    def add_log(self,
        event_name,
        **kwargs):
        kwargs['details'] = json.dumps(kwargs['details'])
        track_event(event_name, kwargs)
        return