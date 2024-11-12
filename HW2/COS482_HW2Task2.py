import psycopg2
from tqdm import tqdm 

# Kaleb Hannan
# HW2 SQL Task 2
# 11/04/2024
# Homework for Universisty of Maine's COS 482

# Connection string

conn = psycopg2.connect("host=localhost dbname=moviesdb user=postgres password=5313")
cur = conn.cursor()

# Add Person Table 
cur.execute('''
            CREATE TABLE IF NOT EXISTS Person(
            id integer NOT NULL PRIMARY KEY,
            fname text,
            lname text,
            gender CHAR(1)
        );    
    ''')
conn.commit()

# Add Movie Table
cur.execute('''
            CREATE TABLE IF NOT EXISTS Movie(
            id integer NOT NULL PRIMARY KEY,
            name text,
            year integer,
            rank NUMERIC
        );
''')
conn.commit()

# Add Director Table
cur.execute('''
            CREATE TABLE IF NOT EXISTS Director(
            id integer PRIMARY KEY,
            fname text,
            lname text
        );
''')
conn.commit()

# Add ActsIn table
cur.execute('''
            CREATE TABLE IF NOT EXISTS ActsIn(
            pid integer,
            mid integer,
            role text,
            PRIMARY KEY (pid,mid),
            CONSTRAINT FK_PersonActIn FOREIGN KEY(pid) REFERENCES Person(id) ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT FK_ActInMovie FOREIGN KEY(mid) REFERENCES Movie(id) ON UPDATE CASCADE ON DELETE RESTRICT
            );
''')
conn.commit()

# Add Directs table
cur.execute('''
            CREATE TABLE IF NOT EXISTS Directs(
            did integer,
            mid integer,
            PRIMARY KEY (did,mid),
            CONSTRAINT FK_DirectorDirects FOREIGN KEY(did) REFERENCES Director(id) ON UPDATE CASCADE ON DELETE RESTRICT,
            CONSTRAINT FK_DirectsMovie FOREIGN KEY(mid) REFERENCES Movie(id) ON UPDATE CASCADE ON DELETE RESTRICT
            );
''')
conn.commit()

# Add Data to tables

# Add data to person table
with open('HW2/IMDB/IMDBPerson.txt', 'r',  encoding='latin-1') as file:
    #skip first line with file lables
    file.readline()
    
    for line in tqdm(file, total= 817718):
        splitString = line.strip().split(',')
        ID = splitString[0]
        fName = splitString[1]
        lName = splitString[2]
        gender = splitString[3]

        try:
            cur.execute("INSERT INTO Person VALUES (%s, %s, %s, %s)",
                (ID, fName, lName, gender))
            conn.commit()
        except:
            conn.rollback()
            print("Error ID:"+ ID)        


# Add data to director table      
with open('HW2/IMDB/IMDBDirectors.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()

    for line in tqdm(file, total= 86880):
        splitString = line.strip().split(',')
        ID = splitString[0]
        fName = splitString[1]
        lName = splitString[2]

        try:
            cur.execute("INSERT INTO Director VALUES (%s, %s, %s)",
                (ID, fName, lName))
            conn.commit()
        except:
            conn.rollback()
            print("Error ID:"+ ID) 


# Add data to Movie table
with open('HW2/IMDB/IMDBMovie.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()

    for line in tqdm(file, total= 388269):
        splitString = line.strip().split('),')
        frontOfRow = splitString[0] + ')'
        backOfRow = splitString[1].split(',')
        frontOfRow = frontOfRow.split(',', 1)
        ID = frontOfRow[0]
        name = frontOfRow[1]
        year = backOfRow[0]
        rank = backOfRow[1] if backOfRow[1] !="" else None

        try:
            cur.execute("INSERT INTO Movie VALUES (%s, %s, %s, %s)",
                (ID, name, year, rank))
            conn.commit()
        except:
            conn.rollback()
            print("Error ID:"+ ID) 


# Add data to ActsIn table
with open('HW2/IMDB/IMDBCast.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()

    for line in tqdm(file, total= 3432630):
        splitString = line.split(',')
        pid = splitString[0]
        mid = splitString[1]
        role = splitString[2] if splitString[2] !="" else None
        if role != None:
            role = role.replace('[','').replace(']','').strip()
        try:
            cur.execute("INSERT INTO actsin VALUES (%s, %s, %s)",
                (pid, mid, role))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)


# Add data to Directs table
with open('HW2/IMDB/IMDBMovie_Directors.txt', 'r', encoding='latin-1') as file:
    #skip the first line with file lables
    file.readline()

    for line in tqdm(file, total= 406967):
        splitString = line.split(',')
        pid = splitString[0]
        mid = splitString[1]

        try:
            cur.execute("INSERT INTO directs VALUES (%s, %s)",
                (pid, mid))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)   

cur.close()
conn.close()