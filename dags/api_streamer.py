import json
import requests
import datetime
from kafka import KafkaProducer
import time
import random
from datetime import datetime as dt

# this is a mock/fake api for this streaming project but this is sufficient to test our expertise 
api_url = "https://randomuser.me/api/"
now = dt.now()  # current date and time

# >> Below will be the json payload and its structure, lets understand this first so we can process this object to kafka streams
# {
#    "gender": "male",
#    "name": {
#       "title": "Monsieur",
#       "first": "Mohamed",
#       "last": "Morel"
#    },
#    "location": {
#       "street": {
#          "number": 964,
#          "name": "Rue de L'Abb\u00e9-Groult"
#       },
#       "city": "Dardagny",
#       "state": "Basel-Stadt",
#       "country": "Switzerland",
#       "postcode": 7797,
#       "coordinates": {
#          "latitude": "-60.8394",
#          "longitude": "-21.4748"
#       },
#       "timezone": {
#          "offset": "-6:00",
#          "description": "Central Time (US & Canada), Mexico City"
#       }
#    },
#    "email": "mohamed.morel@example.com",
#    "login": {
#       "uuid": "bb31832d-88ec-46a3-92f3-3f23c2c430d1",
#       "username": "yellowelephant323",
#       "password": "bigmike",
#       "salt": "GF5XzOpK",
#       "md5": "6ad81f7f14f54761b07bbd743d13f772",
#       "sha1": "b3156c41236bebf14045f4072c4fd6dbb4c132ad",
#       "sha256": "83a38a440e1b7fa99a2462e0899a7f0df5b9cc36f9c0750c7d32acaf331cbb44"
#    },
#    "dob": {
#       "date": "2000-03-27T10:52:01.931Z",
#       "age": 23
#    },
#    "registered": {
#       "date": "2005-08-04T21:34:30.507Z",
#       "age": 18
#    },
#    "phone": "075 721 08 07",
#    "cell": "075 851 16 48",
#    "id": {
#       "name": "AVS",
#       "value": "756.1148.8063.91"
#    },
#    "picture": {
#       "large": "https://randomuser.me/api/portraits/men/23.jpg",
#       "medium": "https://randomuser.me/api/portraits/med/men/23.jpg",
#       "thumbnail": "https://randomuser.me/api/portraits/thumb/men/23.jpg"
#    },
#    "nat": "CH"
# }


# below will be addon to the api that we are using for realtime, just to add more attributes value randomly
sensor_id = []
sensor_type = ['RFID', 'Mobiles', 'Smart Watch', 'Mac Address', 'Tag']
city_list = ['Makkah', 'Madinah']

blood_group = ['A+', 'A-', 'B+', 'B-', 'O+', 'AB+']
pulse_rate = [98,99,100,105,108,110,112]
respiration_rate = [12,13,14,15,16,17,18]
body_temperature = [97,98,99,100,101,102]

def format_json_payload(json_payload):
    # lets extract the requi
    data = {}
    # this will be random data generation
    sensor_information = {}
    sensor_selection = random.choice(sensor_type)
    sensor_information['sensor_id'] = now.strftime("%m%d%Y%H%M%S%f") + '_' + sensor_selection
    sensor_information['sensor_type'] = sensor_selection
    
    vital_signs = {}
    vital_signs['blood_group'] = random.choice(blood_group)
    vital_signs['pulse_rate'] = random.choice(pulse_rate)
    vital_signs['respiration_rate'] = random.choice(respiration_rate)
    vital_signs['body_temperature'] = random.choice(body_temperature)
     
    ts = time.time()
    data['first_name'] = json_payload['name']['first']
    data['last_name'] = json_payload['name']['last']
    data['gender'] = json_payload['gender']
    data['email'] = json_payload['email']
    data['dob'] = json_payload['dob']['date']
    data['phone'] = json_payload['phone']
    data['country'] = "KSA" #json_payload['location']['country']
    data['city'] = random.choice(city_list) #json_payload['location']['city']
    # data['postcode'] = json_payload['location']['postcode']
    data['street_number'] = json_payload['location']['street']['number']
    data['sensor_data'] = sensor_information
    data['vital_signs'] = vital_signs
    data['latitude'] = json_payload['location']['coordinates']['latitude']
    data['longitude'] = json_payload['location']['coordinates']['longitude']
    data['insertion_timestamp'] = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    
    return data
    


def api_streaming():
    streams = requests.get(api_url)
    if streams:
        streams_result = streams.json()
        # we need the data that is coming index = 0 of object result
        streams_subset = streams_result['results'][0]
        
        # now lets use the function json.dumps()  and it will convert a subset of Python objects into a json string
        # indent is a parameter that allows us to pretty print the JSON data 
        print(json.dumps(streams_subset, indent=3))
        source_json_str = json.dumps(streams_subset).encode('utf-8') # json.dumps take a dictionary as input and returns a string as output.
        source_json_dic = json.loads(source_json_str) # json.loads take a string as input and returns a dictionary as output.
        
        # lets format the data and extract nested attributes
        disinfected_streams = format_json_payload(source_json_dic) # you can use this fot your need, but i am leaving it here as we donot need this

        
        return disinfected_streams



def kafka_producer(streams):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    producer.send('users_stream', json.dumps(streams).encode('utf-8'))
    

kafka_producer(api_streaming())
    