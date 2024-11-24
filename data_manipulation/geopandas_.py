def haversine_np(
    longitude1: float, latitude1: float, longitude2: float, latitude2: float
) -> float:
    """Calculates the great circle distance between two points on Earth.

    Args:
        longitude1 (float): Longitude of the first point in decimal degrees.
        latitude1 (float): Latitude of the first point in decimal degrees.
        longitude2 (float): Longitude of the second point in decimal degrees.
        latitude2 (float): Latitude of the second point in decimal degrees.

    Returns:
        float: Distance between the points in kilometers.

    Examples:
        >>> haversine_np(-0.127758, 51.507351, 103.819836, 1.352083)  # London to Singapore
        10880.39...

    Note:
        Uses the Haversine formula to calculate great circle distances.
        Earth radius is assumed to be 6371 km.
        Reference: https://stackoverflow.com/a/4913653
        Reference: https://stackoverflow.com/a/29546836
    """
    import numpy as np

    lon1, lat1, lon2, lat2 = map(
        np.radians, [longitude1, latitude1, longitude2, latitude2]
    )

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371 * c
    return km


if __name__ == "__main__":
    import doctest

    doctest.testmod()
