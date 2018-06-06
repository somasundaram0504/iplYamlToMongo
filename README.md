# iplYamlToMongo
9 yrs of IPL (Indian Premier League) data from 2008 to 2017 available as YAML in "https://cricsheet.org/" is imported and loaded into MongoDB for data analytics using Python.

This project is to load the 636 YAML data files (IPL matches) into MongoDB and use the raw data for Data Analytics. Each YAML file consists of information starting from toss till the MOM presentation. The data is divided into two portions as below for analytical purposes -
  First Portion - Match Level details containing the Venue, teams, when played, who won the toss,match and MOM etc.
  Second Portion - Ball by Ball detail data of First and Second Innings of the match played.

The loading process into MongoDB is written in Python. As mentioned above the two portions of data are loaded into two collections with match level details and ball by ball instructions as documents. 

Things to Remember before execution of py script:
1.Please download the YAML files from https://cricsheet.org/downloads/ (Indian Premier League) 
2.This data is loaded into local host of MongoDB.Please set up the MongoDB in your local machine and keep it up and running before you execute the py script.If you wish to load data into any existing MongoDB servers, please change it to corresponding connection strings in the py file.
2. Execute this python script in the location where  the YAML files are located. Since this is the inital version of the code, I havent implemented the input path to get the YAML files.

Now the data is loaded into MongoDB and then the data analysis on IPL starts. Will include another py file soon on data analysis over IPL data in MongoDB.
