import psycopg2

# Kaleb Hannan
# HW2 SQL Task 2
# 11/04/2024
# Homework for Universisty of Maine's COS 482

# Connection string

conn = psycopg2.connect("host=localhost dbname=moviesdb user=postgres password=5313")
cur = conn.cursor()