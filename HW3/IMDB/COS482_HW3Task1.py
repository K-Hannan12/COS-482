import pymongo
from tqdm import tqdm 

client = pymongo.MongoClient("localhost", 27017)
db = client["COS482HW3"]
colection =  db["IMDB"]

# create and idex for easy acces when looking up my name
colection.create_index([("name", pymongo.ASCENDING)])

# make Cast info dict
castInfo = {}
with open('HW3/IMDB/IMDBCast.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()
    for line in tqdm(file, total= 3432630):
        splitString = line.split(',')
        pid = splitString[0].strip()
        mid = splitString[1].strip()
        role = splitString[2].strip() if splitString[2] !="" else None
        if role != None:
            role = role.replace('[','').replace(']','').strip()
        
        if mid not in castInfo:
            castInfo[mid] = []
        castInfo[mid].append((pid, role))

# create person dict to find personID quikly
personInfo = {}
with open('HW2/IMDB/IMDBPerson.txt', 'r',  encoding='latin-1') as file:
    #skip first line with file lables
    file.readline()
    
    for line in tqdm(file, total= 817718):
        splitString = line.strip().split(',')
        ID = splitString[0].strip()
        fName = splitString[1].strip()
        lName = splitString[2].strip()
        gender = splitString[3].strip()
    
        if ID not in personInfo:
            personInfo[ID] = []
        personInfo[ID].append((fName,lName,gender))

#create dict for Directs info
directsInfo = {}
with open('HW2/IMDB/IMDBMovie_Directors.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()

    for line in tqdm(file, total= 406967):
        splitString = line.split(',')
        did = splitString[0].strip()
        mid = splitString[1].strip()

        if mid not in directsInfo:
                directsInfo[mid] = []
        directsInfo[mid].append(did)

#Create director dict
directorInfo = {}
with open('HW2/IMDB/IMDBDirectors.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()

    for line in tqdm(file, total= 86880):
        splitString = line.strip().split(',')
        ID = splitString[0].strip()
        fName = splitString[1].strip()
        lName = splitString[2].strip()
        if ID not in directorInfo:
            directorInfo[ID] = []
        directorInfo[ID].append((fName,lName))


moive_list = []

#add movies to noSQL db
with open('HW3/IMDB/IMDBMovie.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()

    for line in tqdm(file, total= 388269):
        splitString = line.strip().split('),')
        frontOfRow = splitString[0] + ')'
        backOfRow = splitString[1].split(',')
        frontOfRow = frontOfRow.split(',', 1)
        ID = frontOfRow[0].strip()
        name = frontOfRow[1].strip()
        year = backOfRow[0].strip()
        rank = backOfRow[1].strip() if backOfRow[1] !="" else None

        # get cast
        cast_list = []
        for pid, role in castInfo.get(ID,[]):
            if pid == None:
                continue
            person = personInfo[pid]
            fname = person[0][0]
            lname = person[0][1]
            gender = person[0][2]

            cast = {
                "id": pid,
                "fname": fname,
                "lname": lname,
                "gender": gender,
                "role": role
            }           
            cast_list.append(cast)

        # get directors
        directors_list = []
        for did in directsInfo.get(ID,[]):
            if did == None:
                continue
            director = directorInfo[did]
            fname = director[0][0]
            lname = director[0][1]

            director = {
                "id": did,
                "fname": fname,
                "lname": lname
            }
            directors_list.append(director)
    
        #create moive document
        moive = {
        "_id": name,
        "year": year,
        "rank": rank,
        "cast": cast_list,
        "directors": directors_list
        }
        moive_list.append(moive)

colection.insert_many(moive_list)           
