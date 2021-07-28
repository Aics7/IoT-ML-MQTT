#ok
#impoting necessary libraries that will be used
import paho.mqtt.client as mqtt
import time
import threading
import csv

#setting server to local host
MQTT_SERVER = "localhost"

#creating paths for all topics to be subcribed to
MQTT_PATH1 ="IoTClass/devices/temp1"
MQTT_PATH2 ="IoTClass/devices/temp2"
MQTT_PATH3 ="IoTClass/devices/ldr"
MQTT_PATH4 ="IoTClass/devices/heater"
MQTT_PATH5 ="IoTClass/devices/humidity1"
MQTT_PATH6 ="IoTClass/devices/humidity2"
MQTT_PATH7 ="IoTClass/devices/pump"

#creating lists to store data for each topic
temp1=[]
temp2=[]
ldr=[]
heater=[]
humidity1=[]
humidity2=[]
pump=[]

#creaing on_connect, on_message and a method that'll help in multithreading


#This is for temp1
# The callback when conected.
def on_connect1(client, userdata, flags, rc):
    print("Connected with result code "+str(rc)) 
    client.subscribe(MQTT_PATH1)
    
# Callback when message received
def on_message1(client, userdata, msg):
    pubMessage=msg.payload.decode('utf-8')
    temp1.append(pubMessage)
    print("temp1: "+str(pubMessage))
    
#the method to be ran as a thread
def subscriber_thread1(x):
    client1 = mqtt.Client()
    client1.on_connect = on_connect1
    client1.on_message = on_message1
    client1.connect(MQTT_SERVER, 1883, 60)
    client1.loop_forever()



#This is for temp2
# The callback when conected.
def on_connect2(client, userdata, flags, rc):
    print("Connected with result code "+str(rc)) 
    client.subscribe(MQTT_PATH2)
    
# Callback when message received
def on_message2(client, userdata, msg):
    pubMessage=msg.payload.decode('utf-8')
    temp2.append(pubMessage)
    print("temp2: "+str(pubMessage))

#the method to be ran as a thread
def subscriber_thread2(x):
    client2 = mqtt.Client()
    client2.on_connect = on_connect2
    client2.on_message = on_message2
    client2.connect(MQTT_SERVER, 1883, 60)
    client2.loop_forever()



#This is for ldr
# The callback when conected.
def on_connect3(client, userdata, flags, rc):
    print("Connected with result code "+str(rc)) 
    client.subscribe(MQTT_PATH3)
    
# Callback when message received
def on_message3(client, userdata, msg):
    pubMessage=msg.payload.decode('utf-8')
    ldr.append(pubMessage)
    print("ldr: "+str(pubMessage))

#the method to be ran as a thread
def subscriber_thread3(x):
    client3 = mqtt.Client()
    client3.on_connect = on_connect3
    client3.on_message = on_message3
    client3.connect(MQTT_SERVER, 1883, 60)
    client3.loop_forever()



#This is for heater
# The callback when conected.
def on_connect4(client, userdata, flags, rc):
    print("Connected with result code "+str(rc)) 
    client.subscribe(MQTT_PATH4)
    
# Callback when message received
def on_message4(client, userdata, msg):
    pubMessage=msg.payload.decode('utf-8')
    heater.append(pubMessage)
    print("heater: "+str(pubMessage))

#the method to be ran as a thread
def subscriber_thread4(x):
    client4 = mqtt.Client()
    client4.on_connect = on_connect4
    client4.on_message = on_message4
    client4.connect(MQTT_SERVER, 1883, 60)
    client4.loop_forever()



#This is for humidity1
# The callback when conected.
def on_connect5(client, userdata, flags, rc):
    print("Connected with result code "+str(rc)) 
    client.subscribe(MQTT_PATH5)
    
# Callback when message received
def on_message5(client, userdata, msg):
    pubMessage=msg.payload.decode('utf-8')
    humidity1.append(pubMessage)
    print("humidity1: "+str(pubMessage))

#the method to be ran as a thread
def subscriber_thread5(x):
    client5 = mqtt.Client()
    client5.on_connect = on_connect5
    client5.on_message = on_message5
    client5.connect(MQTT_SERVER, 1883, 60)
    client5.loop_forever()



#This is for humidity2
# The callback when conected.
def on_connect6(client, userdata, flags, rc):
    print("Connected with result code "+str(rc)) 
    client.subscribe(MQTT_PATH6)
    
# Callback when message received
def on_message6(client, userdata, msg):
    pubMessage=msg.payload.decode('utf-8')
    humidity2.append(pubMessage)
    print("humidity2: "+str(pubMessage))

#the method to be ran as a thread
def subscriber_thread6(x):
    client6 = mqtt.Client()
    client6.on_connect = on_connect6
    client6.on_message = on_message6
    client6.connect(MQTT_SERVER, 1883, 60)
    client6.loop_forever()



#This is for pump
# The callback when conected.
def on_connect7(client, userdata, flags, rc):
    print("Connected with result code "+str(rc)) 
    client.subscribe(MQTT_PATH7)
    
# Callback when message received
def on_message7(client, userdata, msg):
    pubMessage=msg.payload.decode('utf-8')
    pump.append(pubMessage)
    print("pump: "+str(pubMessage))

#the method to be ran as a thread
def subscriber_thread7(x):
    client7 = mqtt.Client()
    client7.on_connect = on_connect7
    client7.on_message = on_message7
    client7.connect(MQTT_SERVER, 1883, 60)
    client7.loop_forever()



#Creating and starting threads for methods that subcribe to all seven topics
x1 =threading .Thread (target =subscriber_thread1 ,args =(1 ,),daemon =True )
x1 .start ()
x2 =threading .Thread (target =subscriber_thread2 ,args =(2 ,),daemon =True )
x2 .start ()
x3 =threading .Thread (target =subscriber_thread3 ,args =(3 ,),daemon =True )
x3 .start ()
x4 =threading .Thread (target =subscriber_thread4 ,args =(4 ,),daemon =True )
x4 .start ()
x5 =threading .Thread (target =subscriber_thread5 ,args =(5 ,),daemon =True )
x5 .start ()
x6 =threading .Thread (target =subscriber_thread6 ,args =(6 ,),daemon =True )
x6 .start ()
x7 =threading .Thread (target =subscriber_thread7 ,args =(7 ,),daemon =True )
x7 .start ()
#Runs for 47 minutes just to be safe because the publisher runs for 45 minutes
time .sleep (47 *60 );
print ("\n\n\nThreads done, now writing to csv...\n...\n...")


#Finding lengths of lists that stored the data for each topic
a=len(temp1)
b=len(temp2)
c=len(ldr)
d=len(heater)
e=len(humidity1)
f=len(humidity2)
g=len(pump)

#initializing values for each topic.
ai=0
bi=0
ci=0
di="OFF"
ei=0
fi=0
gi="OFF"

#finding the max length of the lists.
maxLen=max(a,b,c,d,e,f,g)

#creating a csv file named 'iotFinal.csv' for writing
with open('iotFinal.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    #writing the headings at the top
    writer.writerow(["temp1", "temp2", "ldr", "heater", "humidity1", "humidity2", "pump"])
    #runs until content of the list with the highest length is exhausted
    for i in range(maxLen):
        if(i<a):
            ai=temp1[i]

        if(i<b):
            bi=temp2[i]

        if(i<c):
            ci=ldr[i]

        if(i<d):
            di=heater[i]

        if(i<e):
            ei=humidity1[i]

        if(i<f):
            fi=humidity2[i]

        if(i<g):
            gi=pump[i]
        #the above if statements check to see if there is still data in the lists
        #If a list has no data, the most recent data is maintained for that topic

        #writing data to the csv file
        writer.writerow([ai,bi,ci,di,ei,fi,gi])

print ("\n...\nAll set, csv file ready!\n\n")
#END OF CODE
#Issifu

