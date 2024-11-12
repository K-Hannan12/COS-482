import psycopg2
import csv

# Kaleb Hannan
# HW2 SQL Task 4
# 11/04/2024
# Homework for Universisty of Maine's COS 482

# Connection string

conn = psycopg2.connect("host=localhost dbname=moviesdb user=postgres password=5313")
cur = conn.cursor()

"""
Finds the best ranked moives beteen the two years and saves them in a CSV file.

Args:
    k (string or int): Number Moives returned
    start_year (string or int): The start year that you want to get the best movie to be from
    end_year (string or int):The end year that you want to get the best movie to be from

Return:
    None: It will save the results in a CSV file
"""

def find_best_movie_in_years(k, start_year, end_year):
    conn = psycopg2.connect("host=localhost dbname=moviesdb user=postgres password=5313")
    cur = conn.cursor() 

    save_file = "bestMoive.csv"

    cur.execute('''SELECT movie.id AS "ID", movie.name AS "Name", movie.year AS "Year", movie.rank as "Rank"
                FROM movie
                WHERE movie.year BETWEEN {start_year} AND {end_year}
                AND movie.rank IS NOT NULL
                ORDER BY movie.rank DESC
                LIMIT {k};
                '''.format(start_year=start_year,end_year=end_year,k=k)
                )
    
    result = cur.fetchall()

    with open(save_file, mode='w',newline='',encoding='latin-1') as file:
        writer = csv.writer(file,delimiter=';')
        for row in result:
            writer.writerow(row)

find_best_movie_in_years(20,1995,2004)
    