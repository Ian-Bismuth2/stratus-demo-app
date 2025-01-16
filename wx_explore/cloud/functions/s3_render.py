from PIL import Image

import collections
import io
import matplotlib.colors
import numpy
import pygrib
import requests
import tempfile
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Optional

from wx_explore.cloud.proxy import HttpRequest, HttpResponse, proxy

# val0, r0, g0, b0; val1, r1, g1, b1; ...
ColorMapEntry = collections.namedtuple('ColorMapEntry', ['val', 'r', 'g', 'b'])

# Constants for request handling
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.3

def create_session() -> requests.Session:
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def func(req: HttpRequest) -> HttpResponse:
    if not all(param in req.args for param in ['s3_path']):
        return HttpResponse(
            "Missing params",
            code=400,
        )

    session = create_session()
    img_data = None
    grb = None
    
    try:
        with tempfile.NamedTemporaryFile() as f:
            try:
                with session.get(req.args['s3_path'], timeout=REQUEST_TIMEOUT) as resp:
                    resp.raise_for_status()
                    f.write(resp.content)
                    f.flush()
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                return HttpResponse(
                    f"Failed to fetch GRIB data: {str(e)}",
                    code=500,
                )

            try:
                grb = pygrib.open(f.name)
                msg = grb.read(1)[0]
                data = msg.data()[0]
            except Exception as e:
                return HttpResponse(
                    f"Failed to process GRIB data: {str(e)}",
                    code=500,
                )
            finally:
                if grb:
                    grb.close()

            # flip vertically so north is up
            data = numpy.int16(data[::-1])

        try:
            if 'cm' in req.args:
                try:
                    cm_data = [ColorMapEntry(*map(float, cme.split(','))) for cme in req.args['cm'].split(';')]
                except Exception:
                    return HttpResponse(
                        "Malformed color map",
                        code=400,
                    )

                try:
                    norm = matplotlib.colors.Normalize(
                        vmin=min(cme.val for cme in cm_data),
                        vmax=max(cme.val for cme in cm_data),
                    )

                    cdict = {
                        'red': [(norm(cme.val), cme.r/255.0, cme.r/255.0) for cme in cm_data],
                        'green': [(norm(cme.val), cme.g/255.0, cme.g/255.0) for cme in cm_data],
                        'blue': [(norm(cme.val), cme.b/255.0, cme.b/255.0) for cme in cm_data],
                    }

                    cm = matplotlib.colors.LinearSegmentedColormap(
                        'cm',
                        cdict,
                    )

                    if req.args.get('cm_mask_under'):
                        cm.set_under(alpha=0)

                    img = Image.fromarray(numpy.uint8(cm(norm(data))*255), 'RGBA')
                except Exception as e:
                    return HttpResponse(
                        f"Failed to process color map: {str(e)}",
                        code=500,
                    )
            else:
                img = Image.fromarray(data)

            img_data = io.BytesIO()
            try:
                img.save(img_data, format='PNG')
                return HttpResponse(
                    img_data.getvalue(),
                    headers={
                        "Content-Type": "image/png",
                    },
                )
            finally:
                img_data.close()
        except Exception as e:
            return HttpResponse(
                f"Failed to generate image: {str(e)}",
                code=500,
            )


main = proxy(func)