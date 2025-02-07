#!/usr/bin/env python3
from datetime import datetime, timedelta

import logging
import tempfile
import time
import psutil

from wx_explore.common import tracing

# CPU usage threshold (percentage)
CPU_THRESHOLD = 80.0
# Backoff time when CPU is high (seconds)
CPU_BACKOFF_TIME = 30
from wx_explore.common.log_setup import init_sentry
from wx_explore.common.models import Source
from wx_explore.common.tracing import init_tracing
from wx_explore.common.utils import url_exists
from wx_explore.ingest.common import get_queue
from wx_explore.ingest.grib import reduce_grib, ingest_grib_file
from wx_explore.web.core import app, db

logger = logging.getLogger(__name__)


def check_cpu_usage() -> bool:
    """
    Check if CPU usage is above threshold.
    Returns True if CPU usage is acceptable, False if it's too high.
    """
    cpu_percent = psutil.cpu_percent(interval=1)
    if cpu_percent >= CPU_THRESHOLD:
        logger.warning(f"CPU usage too high: {cpu_percent}% >= {CPU_THRESHOLD}%")
        return False
    return True


def ingest_from_queue():
    with app.app_context():
        q = get_queue()
        for ingest_req in q:
            # Queue is empty for now
            if ingest_req is None:
                logger.info("Empty queue")
                break

            ingest_req = ingest_req.data

            # Expire out anything whose valid time is very old (probably a bad request/URL)
            if datetime.utcfromtimestamp(ingest_req['valid_time']) < datetime.utcnow() - timedelta(hours=12):
                logger.info("Expiring old request %s", ingest_req)
                continue

            # If this URL doesn't exist, try again in a few minutes
            if not (url_exists(ingest_req['url']) and url_exists(ingest_req['idx_url'])):
                logger.info("Rescheduling request %s", ingest_req)
                q.put(ingest_req, '5m')
                continue

            # Check CPU usage before processing each item
            if not check_cpu_usage():
                logger.info(f"CPU usage too high, backing off for {CPU_BACKOFF_TIME} seconds before retrying")
                q.put(ingest_req, f"{CPU_BACKOFF_TIME}s")
                time.sleep(1)  # Brief pause before checking next item
                continue

            with tracing.start_span('ingest item') as span:
                # Add CPU monitoring to tracing
                cpu_percent = psutil.cpu_percent()
                span.set_attribute('cpu_usage', cpu_percent)
                
                for k, v in ingest_req.items():
                    span.set_attribute(k, v)

                try:
                    source = Source.query.filter_by(short_name=ingest_req['source']).first()

                    with tempfile.NamedTemporaryFile() as reduced:
                        with tracing.start_span('download') as download_span:
                            logging.info(f"Downloading and reducing {ingest_req['url']} from {ingest_req['run_time']} {source.short_name}")
                            cpu_before = psutil.cpu_percent()
                            reduce_grib(ingest_req['url'], ingest_req['idx_url'], source.fields, reduced)
                            cpu_after = psutil.cpu_percent()
                            download_span.set_attribute('cpu_usage_start', cpu_before)
                            download_span.set_attribute('cpu_usage_end', cpu_after)
                            if cpu_after >= CPU_THRESHOLD:
                                logger.warning(f"High CPU usage during download: {cpu_after}%")
                                
                        with tracing.start_span('ingest') as ingest_span:
                            logging.info("Ingesting all")
                            cpu_before = psutil.cpu_percent()
                            ingest_grib_file(reduced.name, source)
                            cpu_after = psutil.cpu_percent()
                            ingest_span.set_attribute('cpu_usage_start', cpu_before)
                            ingest_span.set_attribute('cpu_usage_end', cpu_after)
                            if cpu_after >= CPU_THRESHOLD:
                                logger.warning(f"High CPU usage during ingest: {cpu_after}%")

                    source.last_updated = datetime.utcnow()

                    db.session.commit()
                except KeyboardInterrupt:
                    raise
                except Exception:
                    logger.exception("Exception while ingesting %s. Will retry", ingest_req)
                    q.put(ingest_req, '4m')


if __name__ == "__main__":
    init_sentry()
    logging.basicConfig(level=logging.INFO)
    init_tracing('queue_worker')
    with tracing.start_span('queue worker'):
        ingest_from_queue()