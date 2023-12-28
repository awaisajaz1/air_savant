import json
import requests
import datetime
from kafka import KafkaProducer
import time
import random
from datetime import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator

# this is a mock/fake api for this streaming project but this is sufficient to test our expertise 
api_url = "https://randomuser.me/api/"
now = dt.now()  # current date and time


default_args = { 
    'owner': 'airsavant',
    'start_date': dt(2023, 12, 28, 10, 00)  
}




# below will be addon to the api that we are using for realtime, just to add more attributes value randomly
sensor_id = []
sensor_type = ['RFID', 'Mobiles', 'Smart Watch', 'Mac Address', 'Tag']

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
    data['country'] = json_payload['location']['country']
    data['city'] = json_payload['location']['city']
    data['postcode'] = json_payload['location']['postcode']
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



def kafka_producer():
    print("stream started!!")
    streams = api_streaming()
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    producer.send('crowd_realtime_stream', json.dumps(streams).encode('utf-8'))
    

# Create your first dag

# with DAG('iot_user_data',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False
#          ) as dag:
    
#     streatimg_task = PythonOperator(
#         task_id='stream_from_api',
#         python_callable=kafka_producer
#     )

### Code is being written and and for the sake of testing i am using sleep for scheduling, it will lift to airflow
secs = [1,2,3]
while True:
    time.sleep(random.choice(secs))
    kafka_producer()
    