# Udacity_Stedi_Human_Balance
The project is based on Spark and Data Lakes topic. The projects aims you to act like a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise;
- has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

## Project Steps
As a data engineer on the STEDI Step Trainer team, I need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Project Data
STEDI has three JSON data sourcee to use from the Step Trainer. Below are the data:
- customer: contains the following fields:
    - serialnumber
    - sharewithpublicasofdate
    - birthday
    - registrationdate
    - sharewithresearchasofdate
    - customername
    - email
    - lastupdatedate
    - phone
    - sharewithfriendsasofdate

- step_trainer: contains the following fields:
    - sensorReadingTime
    - serialNumber
    - birthday
    - distanceFromObject
 
- accelerometer: contains the following fields:
    - timestamp
    - user
    - x
    - y
    - z
