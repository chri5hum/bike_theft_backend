#fuckit, just use v2, get manufacturer and model from title
import requests

from requests.models import Response
def getBikeInfo(date):
    # response = requests.get("http://api.marketstack.com/v1/eod/2021-06-04?access_key=9bc057a36bb2f24e8d89c53af6b0aa7e&symbols=MSFT").json()
    example_request = "https://bikewise.org:443/api/v2/incidents?occurred_after=1622937600&incident_type=theft&proximity=Toronto%2C%20ON&proximity_square=50"

    BIKEWISE_API_URL = "https://bikewise.org:443/api/v2/incidents?"
    DATE = "occurred_after=" + str(int(date))
    OTHER_PARAMS = "&incident_type=theft&proximity=Toronto%2C%20ON&proximity_square=50"

    request_string = BIKEWISE_API_URL + DATE + OTHER_PARAMS
    # request_string = "http://api.marketstack.com/v1/eod/2021-06-04?access_key=9bc057a36bb2f24e8d89c53af6b0aa7e&symbols=MSFT,AAPL"
    response = requests.get(request_string)
    return response.json()['incidents']
    
# test = getBikeInfo((datetime.today() - timedelta(days=4)).timestamp())
# print(test)
def getEventAfterBefore(after, before):
    # response = requests.get("http://api.marketstack.com/v1/eod/2021-06-04?access_key=9bc057a36bb2f24e8d89c53af6b0aa7e&symbols=MSFT").json()
    example_request = "https://bikewise.org:443/api/v2/incidents?occurred_after=1622937600&incident_type=theft&proximity=Toronto%2C%20ON&proximity_square=50"

    BIKEWISE_API_URL = "https://bikewise.org:443/api/v2/incidents?"
    DATE = "occurred_after=" + str(int(after)) + "&occurred_before=" + str(int(before))
    OTHER_PARAMS = "&incident_type=theft&proximity=Toronto%2C%20ON&proximity_square=50"

    request_string = BIKEWISE_API_URL + DATE + OTHER_PARAMS
    # request_string = "http://api.marketstack.com/v1/eod/2021-06-04?access_key=9bc057a36bb2f24e8d89c53af6b0aa7e&symbols=MSFT,AAPL"
    print(request_string)
    response = requests.get(request_string)
    return response.json()['incidents']

def manufacturerExists(m):
    BIKEWISE_API_URL = "https://bikeindex.org:443/api/v3/manufacturers/"
    request_string = BIKEWISE_API_URL + m
    response = requests.get(request_string)
    return not response.status_code == 404

# print(manufacturerExists("All City"))

def getManufacturerFromTitle(title):
    BIKEWISE_MANUFACTURER_API_URL = "https://bikeindex.org:443/api/v3/manufacturers/"

    splitTitle = title.split(' ')
    if splitTitle[0].isdigit():
        splitTitle = " ".join(splitTitle[1:]).split(' ')
    manu = ""
    for s in splitTitle:
        manu += ' ' + s
        request_string = BIKEWISE_MANUFACTURER_API_URL + manu
        response = requests.get(request_string)
        if not response.status_code == 404:
            return response.json()['manufacturer']
    return None

def getByApiURL(url):
    response = requests.get(url)
    return response.json()
# print(getManufacturerFromTitle("norco"))

def getManufacturerFromString(manu):
    BIKEWISE_MANUFACTURER_API_URL = "https://bikeindex.org:443/api/v3/manufacturers/"
    request_string = BIKEWISE_MANUFACTURER_API_URL + manu
    response = requests.get(request_string)
    if not response.status_code == 404:
        return response.json()['manufacturer']
    return None