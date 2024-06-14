from math import nan

import numpy as np


def transform_nan(v):
    if type(v) is float and (np.isnan(v) or v is nan):
        return None
    return v
