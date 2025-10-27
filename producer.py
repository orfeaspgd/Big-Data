import os
import time
import random
import json
from kafka import KafkaProducer
from uxsim import *
import itertools
import datetime
from datetime import timedelta
import pandas as pd
from json import *



KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TEST = os.environ.get("KAFKA_TOPIC_TEST", "vehicle_positionss")
KAFKA_API_VERSION = os.environ.get("KAFKA_API_VERSION", "7.3.1")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
    value_serializer=lambda m: m.encode("utf-8")
)


dftime = datetime.datetime.now()
seed = None
W = World(
    name="",
    deltan=5,
    tmax=3600, #1 hour simulation
    print_mode=1, save_mode=0, show_mode=1,
    random_seed=seed,
    duo_update_time=600
)
random.seed(seed)

# network definition
"""
    N1  N2  N3  N4 
    |   |   |   |
W1--I1--I2--I3--I4-<E1
    |   |   |   |
    v   ^   v   ^
    S1  S2  S3  S4
"""

signal_time = 20
sf_1=1
sf_2=1

I1 = W.addNode("I1", 1, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I2 = W.addNode("I2", 2, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I3 = W.addNode("I3", 3, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I4 = W.addNode("I4", 4, 0, signal=[signal_time*sf_1,signal_time*sf_2])
W1 = W.addNode("W1", 0, 0)
E1 = W.addNode("E1", 5, 0)
N1 = W.addNode("N1", 1, 1)
N2 = W.addNode("N2", 2, 1)
N3 = W.addNode("N3", 3, 1)
N4 = W.addNode("N4", 4, 1)
S1 = W.addNode("S1", 1, -1)
S2 = W.addNode("S2", 2, -1)
S3 = W.addNode("S3", 3, -1)
S4 = W.addNode("S4", 4, -1)

#E <-> W direction: signal group 0
for n1,n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
    W.addLink(n2.name+n1.name, n2, n1, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3, signal_group=0)
    
#N -> S direction: signal group 1
for n1,n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
    W.addLink(n1.name+n2.name, n1, n2, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

#S -> N direction: signal group 2
for n1,n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
    W.addLink(n2.name+n1.name, n2, n1, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)
    

# random demand definition every 30 seconds
dt = 30
demand = 2 #average demand for the simulation time
demands = []
for t in range(0, 3600, dt):
    dem = random.uniform(0, demand)
    for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
        W.adddemand(n1, n2, t, t+dt, dem*0.25)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})
    for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1],[S4, W1]]:
        W.adddemand(n1, n2, t, t+dt, dem*0.75)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})

W.exec_simulation()
W.analyzer.print_simple_stats()

data = W.analyzer.vehicles_to_pandas()

df = pd.DataFrame(columns=['name','origin', 'destination', 'time', 'link', 'position', 'spacing', 'speed'])
for i in range(0, 20):
    new_row = pd.DataFrame({
        'name' : [data ['name'][i]],
        'origin' : [data['orig'][i]],
        'destination' : [data['dest'][i]],
        'time' : [(dftime + timedelta(seconds=int(data['t'][i]))).strftime('%d/%m/%Y %H:%M:%S')],
        'link' : [data['link'][i]],
        'position' : [data['x'][i]],
        'spacing' : [data['s'][i]],
        'speed' :  [data['v'][i]]
    }, index=[i])
    df = pd.concat([df, new_row], ignore_index=True)
    print(new_row[0:20])

save_file = open("savedata.json", "w")  
df.to_json(save_file, orient='records', lines=True, indent=6)
save_file.close()     

#display(df)

i = 0
while i < 20:
    if df.loc[i, 'link'] != 'waiting_at_origin_node':
        producer.send(
            KAFKA_TOPIC_TEST,
            df.loc[[i], :].to_json(orient='records', lines=False).strip('[]'),
        )
    i += 1
    time.sleep(random.randint(1, 5))
    #print(df.to_json(orient='records', lines=True))
producer.flush()