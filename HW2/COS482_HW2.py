import psycopg2
from tqdm import tqdm

# Kaleb Hannan
# HW2 SQL
# 11/04/2024
# Homework for Universisty of Maine's COS 482

# Connection string

conn = psycopg2.connect("host=100.68.143.119 dbname=moviesdb user=kaleb password=5313")
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
            rank REAL
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
with open('HW2/IMDB/IMDBPerson.txt', 'r',  encoding='latin-1') as file:
    total_lines = sum(1 for _ in file)
    total_lines =- 1
    file.seek(0)
    #skip first line with file lables
    file.readline()
    

    for line in tqdm(file, total=total_lines, desc="Inserting Into Person", unit="row"):
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
            
            


cur.close()
conn.close()