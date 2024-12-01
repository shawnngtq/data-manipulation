from typing import TypeVar

import numpy as np
from numpy.typing import NDArray

# Type variable for numeric types (float or numpy array)
NumericType = TypeVar("NumericType", float, NDArray[np.float64])

# Earth's radius in kilometers
EARTH_RADIUS_KM = 6371.0


def haversine_np(
    longitude1: NumericType,
    latitude1: NumericType,
    longitude2: NumericType,
    latitude2: NumericType,
) -> NumericType:
    """Calculates the great circle distance between two points on Earth.

    Args:
        longitude1 (float | ndarray): Longitude of the first point(s) in decimal degrees.
        latitude1 (float | ndarray): Latitude of the first point(s) in decimal degrees.
        longitude2 (float | ndarray): Longitude of the second point(s) in decimal degrees.
        latitude2 (float | ndarray): Latitude of the second point(s) in decimal degrees.

    Returns:
        float | ndarray: Distance between the points in kilometers.

    Raises:
        ValueError: If latitude values are outside [-90, 90] or longitude values outside [-180, 180].

    Examples:
        >>> haversine_np(-0.127758, 51.507351, 103.819836, 1.352083)  # London to Singapore
        10880.39...
        >>> haversine_np(np.array([-0.127758]), np.array([51.507351]),
        ...             np.array([103.819836]), np.array([1.352083]))
        array([10880.39...])

    Note:
        Uses the Haversine formula to calculate great circle distances.
        Earth radius is assumed to be 6371 km.
        Supports both scalar and numpy array inputs for vectorized calculations.
    """
    # Input validation
    for name, val in [("latitude1", latitude1), ("latitude2", latitude2)]:
        if np.any(np.abs(val) > 90):
            raise ValueError(f"{name} must be between -90 and 90 degrees")

    for name, val in [("longitude1", longitude1), ("longitude2", longitude2)]:
        if np.any(np.abs(val) > 180):
            raise ValueError(f"{name} must be between -180 and 180 degrees")

    # Convert to radians
    lon1, lat1, lon2, lat2 = map(
        np.radians, [longitude1, latitude1, longitude2, latitude2]
    )

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Optimize by pre-calculating trigonometric functions
    sin_dlat_2 = np.sin(dlat / 2.0)
    sin_dlon_2 = np.sin(dlon / 2.0)
    cos_lat1 = np.cos(lat1)
    cos_lat2 = np.cos(lat2)

    a = sin_dlat_2**2 + cos_lat1 * cos_lat2 * sin_dlon_2**2
    c = 2 * np.arcsin(np.sqrt(a))

    return EARTH_RADIUS_KM * c


if __name__ == "__main__":
    import doctest

    doctest.testmod()
