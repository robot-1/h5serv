import logging
import time
import sys
import pandas as pd
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderServiceError
from functools import lru_cache
from managedb import Manage
import helpers.retry as hp
#import tqdm
#from tqdm._tqdm_notebook import tqdm_notebook


@hp.retry(exception=Exception, n_tries=4)
@lru_cache
def reverse_geocode(lat, lon):
    """ acquire location address by reverse geocoding.
        searches are stored in local db for caching. 
        return address on success, 0 if None.
    """
    #coordinates = "14.505025, 124.851131" 
    #coordinates = "14.647179, 121.072005"
    db = Manage('addresses.db')
    location = db.check_loc(lat, lon)
    if not location:
        locator = Nominatim(user_agent="test")
        location = locator.reverse("{}, {}".format(lat, lon))
        if not location:
            return (-1,None)
        else:
            return (0, location.address)
    else:
        return (1, location[0])

if __name__ == "__main__":
    lat, lon  = (7.6321118, 123.0303669)
    ret = reverse_geocode(lat, lon)
    if not ret:
        print("location not found.")
    else:
        print(ret)
