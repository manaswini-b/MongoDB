'''
Author: Hasitha Nekkalapu
        Manaswini Budavati
Date:   08/11/2017

For converting to xml we have used an online convertor tool at "https://www.freeformatter.com/"
'''

import pymongo
import pymysql
from pymongo import ReadPreference
from pymongo import MongoClient

#Connecting to mySql
sqldb = pymysql.connect(host='localhost', port=3306, user='root', passwd=<password>, db=<DB name>)
cursor = sqldb.cursor()

#Establish connection to Mongo
client = MongoClient('mongodb://localhost:27017/') #localhost and default port
mongodb = client.db2
project_collection = mongodb.project
dept_collection = mongodb.department


#Get employee, department, workson, project tables from SQL and join them
projectquery="select pname, pnumber, dname, fname, lname, hours from db2.employee JOIN db2.workson on db2.employee.ssn = db2.workson.essn JOIN db2.project on db2.project.pnumber = db2.workson.pno JOIN db2.department on db2.project.dnum = db2.department.dnumber ORDER BY pname;"
cursor.execute(projectquery)
result = cursor.fetchall()
current_project= None
idno = 101
jsondoc ={"projectdata": []}
#convert into JSON
for obj in result:
    if current_project == None:
        idno += 1
        current_project = obj[0]
        jsonobj = {"_id": idno ,"pname" : obj[0], "pnumber" : obj[1], "dname" : obj[2]}
        jsonobj["employees"] = [{"fname" : obj[3],"lname": obj[4], "hours": obj[5]}]
    elif current_project == obj[0]:  
        jsonobj["employees"].append({"fname" : obj[3],"lname": obj[4], "hours": obj[5]})
    else:
        jsondoc["projectdata"].append(jsonobj)
        project_collection.insert(jsonobj)
        idno += 1
        current_project = obj[0]
        jsonobj = {"_id": idno ,"pname" : obj[0], "pnumber" : obj[1], "dname" : obj[2]}
        jsonobj["employees"] = [{"fname" : obj[3],"lname": obj[4], "hours": obj[5]}]
    if(obj == result[-1]):
        #insert in project collection in mongoDB
        jsondoc["projectdata"].append(jsonobj)
        project_collection.insert(jsonobj)
print jsondoc
''''Sample document in project
{ "dname" : "Software", "pnumber" : 22, "employees" : [ { "lname" : "Wolowitz", "hours" : 4, "fname" : "Penny" }, { "lname" : "Geller", "hours" : 30, "fname" : "Zach" } ], "pname" : "SearchEngine" }
'''

#Get employee, department, deptlocations tables from SQL and join them
departmentquery = "select dname, lname, dlocation from db2.employee JOIN db2.department on db2.employee.ssn = db2.department.mgr_ssn JOIN db2.deptlocations on db2.department.dnumber=db2.deptlocations.dnumber;";
cursor.execute(departmentquery)
d_result = cursor.fetchall()
current_dept= None
idnum = 1
jsondoc_dept = {"departmentdata": []}
#convert into JSON
for obj in d_result:
    if current_dept == None:
        idnum += 1
        current_dept = obj[0]
        jsonobj = {"_id": idnum,"dname" : obj[0], "mgr_lname" : obj[1]}
        jsonobj["locations"] = [obj[2]]
    elif current_dept == obj[0]:  
        jsonobj["locations"].append(obj[2])
    else:
        jsondoc_dept["departmentdata"].append(jsonobj)
        dept_collection.insert(jsonobj)
        idnum += 1
        current_dept = obj[0]
        jsonobj = {"_id": idnum,"dname" : obj[0], "mgr_lname" : obj[1]}
        jsonobj["locations"] = [obj[2]]
    if(obj == d_result[-1]):
        #insert in department collection in mongoDB
        jsondoc_dept["departmentdata"].append(jsonobj)
        dept_collection.insert(jsonobj)
print jsondoc_dept
''' Sample document in department
{ "dname" : "Administration", "mgr_lname" : "Wallace", "locations" : [ " 'Stafford'" ] }
'''

#MongoDB Queries
collection=mongodb['project']
cursor = collection.find({})
count = 0
for document in cursor:
    del document["_id"]
    print document
    #Query1 : find number of projects where department name = "Software"
    if(document["dname"]== "Software"):
        count += 1
        #print(document["pnumber"])

    #Query2 : find Fname of employees where pnumber = "22"
    if(document["pnumber"] == 22):
        employees = document["employees"]
        for employee in employees :
            print employee["fname"]
print count
collection=mongodb['department']
cursor = collection.find({})
count = 0
for document in cursor:
    del document["_id"]
    print document
    #Query3 : find number of departments where department name = "Software"
    if(document["dname"]== "Software"):
        count += 1
        #print(document["pnumber"])
print count



