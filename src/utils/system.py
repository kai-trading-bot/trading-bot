import humanize
import psutil

from src.utils.email import Email
from src.utils.opsgenie import OpsGenie


def disk_usage(warning_threshold: float = 0.99, alert_threshold: float = 0.995, alert: bool = True):
    # Threshold is based on 10GB disk.
    usage = psutil.disk_usage('/')
    percent = usage.percent * 0.01
    details = dict(
        Total=humanize.naturalsize(usage.total),
        Used=humanize.naturalsize(usage.used),
        Free=humanize.naturalsize(usage.free),
        Percent=usage.percent
    )
    if alert:
        if warning_threshold < percent < alert_threshold:
            # TODO: send slack alert
            email = Email(f'Disk Usage High')
            email.add_details('Disk Usage High', **details)
            email.send()
        elif percent >= alert_threshold:
            OpsGenie().send('Disk Usage High', description=f'Percent Usage: {percent}', details=details)
    else:
        return details
