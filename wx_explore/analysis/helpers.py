from typing import Iterator, List, Dict, Tuple
import datetime
from wx_explore.common.models import Metric, SourceField, DataPointSet


def get_metric(sfid: int) -> Metric:
    return SourceField.query.get(sfid).metric


def group_by_time(groups: List[List[DataPointSet]]) -> Iterator[Tuple[datetime.datetime, Tuple[DataPointSet, ...]]]:
    """
    Given n lists of data points (one list per source field), return a time
    and a n-tuple of data points which have that valid_time, eliminating times
    where not all source fields have a point for that time.
    """
    pt_by_time: List[Dict[datetime.datetime, DataPointSet]] = []
    for g in groups:
        pt_by_time.append({dp.valid_time: dp for dp in g})

    common_times = set(pt_by_time[0].keys()).intersection(*[set(d.keys()) for d in pt_by_time[1:]])

    for t in sorted(common_times):
        yield (t, tuple(d[t] for d in pt_by_time))


import math

def distance_weighted_interpolate(array, x, y):
    """
    Performs inverse distance weighted interpolation on a 2D array at point (x,y)
    using Cartesian distances to the 4 nearest points.

    Parameters:
    array: 2D numpy array
    x, y: floating point coordinates within the array bounds

    Returns:
    Interpolated value at (x,y)
    """
    # Get the four surrounding integer coordinates
    x1, y1 = int(x), int(y)
    x2, y2 = x1 + 1, y1 + 1

    # Get the four surrounding values and their distances
    points = [
        (x1, y1, array[y1][x1]),
        (x1, y2, array[y2][x1]),
        (x2, y1, array[y1][x2]),
        (x2, y2, array[y2][x2])
    ]

    # Calculate distances and weights
    distances = []
    values = []
    for px, py, val in points:
        # Compute Euclidean distance
        dist = math.sqrt((x - px)**2 + (y - py)**2)
        distances.append(dist)
        values.append(val)

    # Convert distances to weights (inverse distance)
    weights = [1/d for d in distances]

    # Normalize weights to sum to 1
    weight_sum = sum(weights)
    weights = [w/weight_sum for w in weights]

    # Calculate weighted average
    result = sum(w * v for w, v in zip(weights, values))

    return result
