from airflow.providers.http.operators.http import HttpHook
import pandas as pd
import pycountry

us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'American Samoa': 'AS',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Guam': 'GU',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))

def get_average_temp(http_hook:HttpHook, dataset_id:str, location_ids:list, start_date:str, end_date:str, datatype_id:str, units:str, chunk_size:int = 1000):
    df = pd.DataFrame()
    for location_name in location_ids:
        request_data = {
                "datasetid": dataset_id, 
                "locationid": location_ids[location_name],  
                "startdate": start_date, 
                "enddate": end_date, 
                "datatypeid": datatype_id, 
                "units": units} 
        df_next = get_paged_ncdc_results(http_hook, "data", request_data)
        df_next["location_name"] = location_name
        df_next["location_id"] = location_ids[location_name]
        if len(df.index) == 0:
            df = df_next
        else:
            df = df.append(df_next, ignore_index=True)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df = df.groupby(['date', "location_name"]).mean()
    df = df.rename(columns={'value':'average_temp'})
    return df

def get_city_ids(http_hook:HttpHook, cities:list):
    locations = []
    for city in cities:
        locations.append(get_location_string(city["country"], city["name"], city["state"]))
    df = get_paged_ncdc_results(http_hook=http_hook, endpoint="locations", request_data={"locationcategoryid": "CITY"})
    df = df.set_index("name")
    result = {}
    for location in locations:
        try:
            location_id = df.at[location, "id"]
            result[location] = location_id
        except KeyError:
            result[location] = None
    return result

def get_paged_ncdc_results(http_hook:HttpHook, endpoint:str, request_data:dict, chunk_size:int=1000):
    limit = chunk_size
    offset = 1
    count = 1
    df = pd.DataFrame()
    while offset <= count:
        request_data["offset"] = offset
        request_data["limit"] = chunk_size
        response = http_hook.run(endpoint, data=request_data)
        df_next = pd.json_normalize(response.json()['results'])
        if len(df.index) == 0:
            df = df_next
        else:
            df = df.append(df_next, ignore_index=True)
        offset = len(df.index) + 1
        count = response.json()["metadata"]["resultset"]["count"]
        print("downloaded {} of {} records".format(offset-1, count))
    return df

def get_location_string(country:str, city:str, us_state:str, ):
    country_obj = pycountry.countries.get(name=country)
    if country_obj is None:
        return None
    if us_state:
        us_state_alpha_2 = us_state_abbrev[us_state]
        return "{}, {} {}".format(city, us_state_alpha_2, country_obj.alpha_2)
    return "{}, {}".format(city, country_obj.alpha_2)