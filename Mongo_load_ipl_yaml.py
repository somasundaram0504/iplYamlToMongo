import os
import yaml
import datetime
import time
from pymongo import MongoClient
from pymongo import write_concern

connection = MongoClient("mongodb://localhost")
matches_list = []
db = connection.ipl
matches = db.matches        
iplBallStats = db.iplBallStats

def load_ipl_data():
    try:                
        dropCollection = matches.drop()
        yaml_file_list = [file for file in os.listdir("d:/self analysis/ipl") if file.endswith(".yaml")]
        print len(yaml_file_list)
        log_print_time()        
        for fileName in yaml_file_list:
            yamlFile = open(fileName,"r")
            docs = list(yaml.load_all(yamlFile))
            value = [value for doc in docs for key,value in doc.iteritems() if(key == "info")]
            #using the filename as _id field 
            value[0]["_id"] = fileName.replace(".yaml","")
            #convert the datetime objects into string due to BSON error while importing into MongoDB
            value[0]["dates"] = [str(date_object) for date_object in value[0]["dates"]]   
            value[0]["season"] = [datetime.datetime.strptime(date_object,'%Y-%m-%d').year for date_object in value[0]["dates"]][0] 
            matches_list.append(value[0])                                                
        log_print_time()
        matches_insert = matches.insert_many(matches_list)                                     
        print matches_insert.acknowledged
        log_print_time()
        print "Insert successful"
    except Exception as ex:
        print "unexpected error occurreced",type(ex),ex
            
def load_ball_by_ball_data():
    try:    
        dropCollection = iplBallStats.drop()
        yaml_file_list = [file for file in os.listdir("d:/self analysis/ipl") if file.endswith(".yaml")]
        match_count = 0
        log_print_time()
        for fileName in yaml_file_list:
            print fileName
            yamlFile = open(fileName,"r")
            docs = list(yaml.load_all(yamlFile))
            #FileName is considered as _id in matches collection
            first_innings_data = getInnings_balldata(docs,'1st innings',fileName.replace(".yaml",""))            
            second_innings_data = getInnings_balldata(docs,'2nd innings',fileName.replace(".yaml",""))            
            #Insert first and second innings data separately
            match_count += 1
            if(first_innings_data !=[]):
                first_inn_insert = iplBallStats.insert_many(first_innings_data) 
            else:
                print "1st innings no data available" 
            if(second_innings_data !=[]):
                sec_inn_insert = iplBallStats.insert_many(second_innings_data)
            else:
                print "2nd innings no data available"                 
            
            print "Match ball data inserts completed", match_count
            log_print_time()  
                                 
    except Exception as ex:
        print "unexpected error occurreced",type(ex),ex

def getInnings_balldata(docs,inningsType,match_id):    

    data = ([t for doc in docs for x,y in doc.iteritems() if x == 'innings' for a in y 
    for k,v in a.iteritems() if k == inningsType for r,t in v.iteritems() if r == 'deliveries'])                                    
    teamName = ([t for doc in docs for x,y in doc.iteritems() if x == 'innings' for a in y 
    for k,v in a.iteritems() if k == inningsType for r,t in v.iteritems() if r == 'team']) 
    if(data!=[]):
        #Get the ball statistics    
        ball_stats = [stats for balls in data[0] for ball,stats in balls.iteritems()]
        #Get the corresponding ball count 
        ball_count = [ball for balls in data[0] for ball,stats in balls.iteritems()]
    
        #adding the playing team, match id reference and type of innings to the balls document
        teams =teamName * len(r[0])  
        matchIds = [match_id]*len(r[0])
        inningsType = [inningsType]*len(r[0])

        ball_data = map(add_ball_count,ball_stats,ball_count,matchIds,inningsType,teams)    
        return ball_data
    else:
        #To return empty list if the second innings or entire match did not played due to bad weather conditions or other reasons
        return []
def add_ball_count(stats_data,ball_count,match_id,inningsType,teamName):
    stats_data['over'] = int(str(ball_count).split('.')[0])+1
    stats_data['ball'] = int(str(ball_count).split('.')[1])
    stats_data['matchId'] = match_id
    stats_data['inningsType'] = inningsType
    stats_data['team'] = teamName
    return stats_data    

def log_print_time():
    ts = time.time()
    print datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')[0:-3]

if __name__ == "__main__":    
    #load_ipl_data()
    load_ball_by_ball_data()