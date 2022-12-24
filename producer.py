import json
import time
import requests

# Run `pip install kafka-python` to install this package
from kafka import KafkaProducer

url = "https://transloc-api-1-2.p.rapidapi.com/vehicles.json"
urlroutes = "https://transloc-api-1-2.p.rapidapi.com/routes.json"
attributes=[] 
l={}

producer = KafkaProducer(bootstrap_servers="localhost:9092")

def produce(agencies, X_RapidAPI_Key, X_RapidAPI_Host):
  querystring = {"agencies":agencies}

  headers = {
  	"X-RapidAPI-Key": X_RapidAPI_Key,
  	"X-RapidAPI-Host": X_RapidAPI_Host	
  }
  response = requests.request("GET", url, headers=headers, params=querystring, timeout=5)
  responseRoutes = requests.request("GET", url=urlroutes, headers=headers, params=querystring)
  Routes = responseRoutes.json()
  agencie = response.json()
  for key in agencie["data"].keys():
    vehicules = agencie["data"][key]
    for vehicule in vehicules:
      l={}
      l["actualtime"]=agencie["generated_on"]
      l["last_updated_on"]=vehicule["last_updated_on"]
      l["speed"]=vehicule["speed"]
      l["call_name"]=vehicule["call_name"]
      l["heading "]=vehicule["heading"]
      l["location"]=vehicule["location"]
      l["lat"]=vehicule["location"]["lat"]
      l["lon"]=vehicule["location"]["lng"]
      l["Original_route_id "]=vehicule["route_id"]
      
      if key in Routes["data"]:
        route = Routes["data"][key]
        routes_served=[]
        
        for r in route:
          if r["route_id"]==vehicule["route_id"]:
            l["route_short_name"]=r["short_name"]
            l["route_long_name"]=r["long_name"]
            l["agencie"]=r["agency_id"]
            l["transport_type"]=r["type"]
        l['routes_served']=routes_served
      else:
        l["route_short_name"]=""
        l["route_long_name"]=""
        l["agencie"]=""
        l["transport_type"]=""
        l['routes_served']=[]

      if vehicule["arrival_estimates"]:
        l["arr_route_id"]=vehicule["arrival_estimates"][-1]["route_id"]
        l["arrival_at"]=vehicule["arrival_estimates"][-1]["arrival_at"]
        l["arr_stop_id"]=vehicule["arrival_estimates"][-1]["stop_id"]
        for r in route:
          if r["route_id"]==vehicule["arrival_estimates"][-1]["route_id"]:
            l["arr_route_name"]=r["long_name"]            
        #producer.send("openapi-vehicule", json.dumps(vehicule).encode())              
      else:
        l["arr_route_id"]=""
        l["arrival_at"]=""
        l["arr_stop_id"]=""
        l["arr_route_name"]=""
      

      producer.send("vehicle", json.dumps(l).encode())
      print(l)
 #  print("At {} the speed of vehicule is {} Kph".format(time.time(), vehicules[0]["speed"]))
  #print("the route of vehicule is {}".format(vehicules[1]["arrival_estimates"]))
while True:
  produce("12,20,32", your_X_RapidAPI_Key, your_X_RapidAPI_Host)
  time.sleep(5)