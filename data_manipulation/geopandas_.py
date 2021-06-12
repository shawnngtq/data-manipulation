def haversine_np(longitude1, latitude1, longitude2, latitude2):
    """
    Return the great circle distance between two points on the earth (specified in decimal degrees). All args must be of equal length. Reference from https://stackoverflow.com/a/4913653, https://stackoverflow.com/a/29546836

    Parameters
    ----------
    longitude1: float
        Base longitude
    latitude1: float
        Base latitude
    longitude2: float
        Longitude to compare
    latitude2: float
        Latitude to compare

    Returns
    -------
    km : float
    """
    import numpy as np

    lon1, lat1, lon2, lat2 = map(np.radians, [longitude1, latitude1, longitude2, latitude2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371 * c
    return km


if __name__ == "__main__":
    import doctest

    doctest.testmod()
