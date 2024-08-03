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

Below is the flowchart to understand flow of data and how it is stored in different zones:

![image](https://github.com/user-attachments/assets/30f4d9de-7531-4bd2-881b-c20065b2a360)

Refer to the diagram below to understand the relationship between entities:

![image](https://github.com/user-attachments/assets/e1f1a3ff-f104-458f-ab5d-8e4844fae52c)

## Project Initiation and Completion steps

### 1. Raw data ingestion of above tables into S3 with proper names:

- customer_landing
- accelerometer_landing
- step_trainer_landing

At this point, its not possible to view the data in tabular format, but one can still see the json structure. To query the data in S3, I need to create glue jobs and then using athena, querying on the tables will be easier

## Landing Zone
### 2. Use Glue Studio to ingest data from S3 bucket with below DDL scripts or add tables manually and point it to the S3 location.
        
 **customer_landing.sql**

![image](https://github.com/user-attachments/assets/e8896522-0195-4b53-a559-322f7814c66e)

**accelerometer_landing.sql**

![image](https://github.com/user-attachments/assets/536a55ba-2953-47d9-b7bd-7102bf7d588b)

**step_trainer_landing.sql**

![image](https://github.com/user-attachments/assets/b145e463-26e5-4d1b-959e-334ed28e0c97)

### 3. Use Athena to query the data and record the count in the tables

**a) Table structure screenshot for customer_landing**

![image](https://github.com/user-attachments/assets/7b0b2be8-f378-4ba8-9eef-13feef0210c9)

**b) Count screenshot for customer_landing**

![image](https://github.com/user-attachments/assets/80a19cd2-6ac4-4a35-a871-5623167cfa4f)

**c) Screenshot of rows where sharewithresearchasofdate is blank**

![image](https://github.com/user-attachments/assets/2d3704a3-a4de-44eb-b414-c891d5ff667a)

**d) Table structure screenshot for accelerometer_landing**

![image](https://github.com/user-attachments/assets/71104d7a-e94b-4902-84c0-24f58b6cbdf5)

**e) Count screenshot for accelerometer_landing**

![image](https://github.com/user-attachments/assets/7c77169b-cb44-412f-a14d-67745f061bbc)

**f) Table structure screenshot for step_trainer_landing**

![image](https://github.com/user-attachments/assets/f2fe9356-58ea-4a6d-8603-cf169d549070)

**g) Count screenshot for step_trainer_landing**

![image](https://github.com/user-attachments/assets/5bcc25eb-a4a1-40f7-9859-ecd4e0c543f7)

## 4. Trusted Zone

**a) Glue Job for creating customer_trusted data in S3**

The customer_landing is filtered for rows where sharewithresearchasofdate!=0, which in this case is available to public.

![image](https://github.com/user-attachments/assets/89fd6f95-f557-461a-bc96-0c9ce389a422)

**b) Using Athena, queried customer_trusted data in S3**

The customer_trusted data has 482 rows, where sharewithresearchasofdate!=0.

![image](https://github.com/user-attachments/assets/01c86602-355a-4b55-86e9-33a81a041297)

**c) Glue Job for creating accelerometer_trusted data in S3**

Sanitize the accelerometer data using accelerometer Readings from customers who agreed to share their data for research purposes (customer_trusted).

![image](https://github.com/user-attachments/assets/c2b04821-ce5f-413e-b9c8-232728d50293)

**d) Using Athena, queried accelerometer_trusted data in S3**

The accelerometer_trusted data has 40981 rows, for customers who agreed to share their data for research purposes.

![image](https://github.com/user-attachments/assets/0189af32-0e4f-4d7b-a343-54816084151b)

**e) Glue Job for creating accelerometer_trusted data in S3**

Populate step_trainer_trusted table that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated). Creation of curated data is shown in the "Curated Zone" section.

![image](https://github.com/user-attachments/assets/661bd35e-5daa-424d-821e-62500d87538f)

**f) Using Athena, queried step_trainer_trusted data in S3**

The step_trainer_trusted data has 14460 rows, for customers who have accelerometer data and have agreed to share their data for research.

![image](https://github.com/user-attachments/assets/f3e8ccbe-83bc-450d-827e-63c349509bdf)

## 5. Curated Zone

**a) Glue Job for creating customer_curated data in S3**

Customers who have accelerometer data and have agreed to share their data for research called customers_curated.

![image](https://github.com/user-attachments/assets/ec90b11c-8570-459d-b798-12a6a65ac598)

**b) Using Athena, queried customer_curated data in S3**

The customer_curated data has 482 rows.

![image](https://github.com/user-attachments/assets/fb5c917f-33e7-41a8-aebd-fd17a981a92c)

**c) Glue Job for creating machine_learning_curated data in S3**

It is an aggregated table that has each of the Step Trainer Readings (step_trainer_trusted), and the associated accelerometer reading data (accelerometer_trusted) for the same timestamp, but only for customers who have agreed to share their data.

![image](https://github.com/user-attachments/assets/eba7f542-efba-4d55-a5d8-552bad3d5a36)

**d) Using Athena, queried machine_learning_curated data in S3**

The machine_learning_curated data has 43681 rows.

![image](https://github.com/user-attachments/assets/3c5bfe77-0705-4000-a997-7045099460ab)
