import psycopg2

# Kaleb Hannan
# HW2 SQL
# 11/04/2024
# Homework for Universisty of Maine's COS 482

# Connection string

conn = psycopg2.connect("host=100.68.143.119 dbname=moviesdb user= password=")
cur = conn.cursor()

# Add Person Table 
cur.execute('''
            CREATE TABLE IF NOT EXISTS Person(
            id integer NOT NULL PRIMARY KEY,
            fname text NOT NULL,
            lname text NOT NULL,
            gender CHAR(1) NOT NULL
        );    
    ''')
conn.commit()

# Add Movie Table
cur.execute('''
            CREATE TABLE IF NOT EXISTS Movie(
            id integer NOT NULL PRIMARY KEY,
            name text NOT NULL,
            year integer NOT NULL,
            rank REAL NOT NULL
        );
''')
conn.commit()

# Add Director Table
cur.execute('''
            CREATE TABLE IF NOT EXISTS Director(
            id integer NOT NULL PRIMARY KEY,
            fname text NOT NULL,
            lname text NOT NULL
        );
''')
conn.commit()

# Add ActsIn table
cur.execute('''
            CREATE TABLE IF NOT EXISTS ActsIn(
            pid integer,
            mid integer,
            role text NOT NULL,
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

cur.close()
conn.close()