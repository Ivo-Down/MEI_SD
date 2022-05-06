import json
import random

number_of_devices = 100
types_of_devices = ['drone', 'car', 'phone', 'fridge']
result = []

for i in range(1,number_of_devices + 1,1):
  result.append({'id': i, 'password': "pw_" + str(i), 'type': random.choice(types_of_devices)})
  
  
# Serializing json 
json_object = json.dumps(result, indent = 4)
  
# Writing to sample.json
with open("Ficheiros/dispositivos.json", "w") as outfile:
    outfile.write(json_object)