import collections
import datetime
import logging
import pygrib

from wx_explore.common import tracing, storage
from wx_explore.common.models import (
    Metric,
    SourceField,
)
from wx_explore.common.utils import get_url
from wx_explore.ingest.common import get_or_create_projection, get_source_module
from wx_explore.web.core import db

logger = logging.getLogger(__name__)


def get_grib_ranges(idxs, source_fields):
    """
    Given an index file, return a list of tuples that denote the start and length of each chunk
    of the GRIB that should be downloaded
    :param idxs: Index file as a string
    :param source: List of SourceField that should be extracted from the GRIB
    :return: List of (start, length)
    """
    offsets = []
    last = None
    for line in idxs.split('\n'):
        tokens = line.split(':')
        if len(tokens) < 7:
            continue

        _, offset, _, short_name, level, _, _ = tokens

        offset = int(offset)

        # NAM apparently has index fields like
        # 624.1:698199214:d=2020020918:UGRD:10 m above ground:5 hour fcst:
        # 624.2:698199214:d=2020020918:VGRD:10 m above ground:5 hour fcst:
        #  so offset != last is needed to make sure we don't try and download anything with length 0
        if last is not None and offset != last:
            offsets.append((last, offset-last))
            last = None

        if any(sf.idx_short_name == short_name and sf.idx_level == level for sf in source_fields):
            last = offset

    return offsets


def reduce_grib(grib_url, idx_url, source_fields, out_f):
    """
    Downloads the appropriate chunks (based on desired fields described by source_fields)
    of the GRIB at grib_url (using idx_url to quickly seek around) and writes the chunks
    to out_f.

    It is assumed that the caller has checked that the URLs exist before this function is called.
    """
    idxs = get_url(idx_url).text
    offsets = get_grib_ranges(idxs, source_fields)

    for offset, length in offsets:
        start = offset
        end = offset + length - 1

        try:
            grib_data = get_url(grib_url, headers={
                "Range": f"bytes={start}-{end}"
            }).content
        except Exception as e:
            logger.exception("Unable to fetch grib data. Continuing anyways...")
            continue

        out_f.write(grib_data)

    out_f.flush()


def get_end_valid_time(msg):
    """
    Gets the valid time for msg, using the end time if the message is an avg
    over a time range
    """
    valid_date = msg.validDate
    if (not msg.valid_key('stepType')) or (msg.stepType == 'instant'):
        return valid_date

    if not msg.valid_key('lengthOfTimeRange'):
        return valid_date

    if msg.fcstimeunits == 'secs':
        return valid_date + datetime.timedelta(seconds=msg.lengthOfTimeRange)
    elif msg.fcstimeunits == 'mins':
        return valid_date + datetime.timedelta(minutes=msg.lengthOfTimeRange)
    elif msg.fcstimeunits == 'hrs':
        return valid_date + datetime.timedelta(hours=msg.lengthOfTimeRange)
    elif msg.fcstimeunits == 'days':
        return valid_date + datetime.timedelta(days=msg.lengthOfTimeRange)

    return valid_date


def ingest_grib_file(file_path, source, max_chunk_size=50*1024*1024):  # 50MB chunk size
    """
    Ingests a given GRIB file into the backend.
    :param file_path: Path to the GRIB file
    :param source: Source object which denotes which source this data is from
    :param max_chunk_size: Maximum size in bytes for message chunks before storage
    :return: None
    """
    logger.info("Processing GRIB file '%s'", file_path)

    grib = None
    try:
        grib = pygrib.open(file_path)

        # Process fields in chunks to manage memory usage
        current_chunk = collections.defaultdict(lambda: collections.defaultdict(list))
        current_chunk_size = 0

        for field in SourceField.query.filter(SourceField.source_id == source.id, SourceField.metric.has(Metric.intermediate == False)).all():
            try:
                msgs = grib.select(**field.selectors)
            except ValueError:
                if field.selectors.get('shortName') not in ('wind', 'wdir'):
                    logger.warning("Could not find message(s) in grib matching selectors %s", field.selectors)
                continue

            for msg in msgs:
                with tracing.start_span('parse message') as span:
                    span.set_attribute('message', str(msg))

                    if field.projection is None or field.projection.params != msg.projparams:
                        projection = get_or_create_projection(msg)
                        field.projection_id = projection.id
                        db.session.commit()

                    valid_date = get_end_valid_time(msg)
                    msg_values = msg.values
                    msg_size = len(msg_values) * 8  # Approximate size in bytes (8 bytes per float)

                    # If adding this message would exceed chunk size, store current chunk
                    if current_chunk_size + msg_size > max_chunk_size and current_chunk:
                        with tracing.start_span('save chunk'):
                            logger.info("Storing chunk of size %d bytes", current_chunk_size)
                            for proj, fields in current_chunk.items():
                                storage.get_provider().put_fields(proj, fields)
                        current_chunk.clear()
                        current_chunk_size = 0

                    current_chunk[field.projection][(field.id, valid_date, msg.analDate)].append(msg_values)
                    current_chunk_size += msg_size

        # Process any derived fields in the final chunk
        with tracing.start_span('generate derived'):
            logger.info("Generating derived fields")
            for proj, fields in get_source_module(source.short_name).generate_derived(grib).items():
                for k, v in fields.items():
                    derived_size = sum(len(arr) * 8 for arr in v)  # 8 bytes per float
                    if current_chunk_size + derived_size > max_chunk_size and current_chunk:
                        # Store current chunk before adding derived fields
                        for proj, fields in current_chunk.items():
                            storage.get_provider().put_fields(proj, fields)
                        current_chunk.clear()
                        current_chunk_size = 0
                    current_chunk[proj][k].extend(v)
                    current_chunk_size += derived_size

        # Store final chunk if any data remains
        if current_chunk:
            with tracing.start_span('save final chunk'):
                logger.info("Storing final chunk of size %d bytes", current_chunk_size)
                for proj, fields in current_chunk.items():
                    storage.get_provider().put_fields(proj, fields)

        logger.info("Done saving denormalized data")

    finally:
        if grib is not None:
            grib.close()