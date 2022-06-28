import psycopg2

# Create the database connection.

#conn = psycopg2.connect("host=yb-tserver-n1 port=5433 dbname=postgres user=postgres password=postgres")
conn = psycopg2.connect(host="127.0.0.1", port=5432, database="dvdrental", user="root", password="root")

# WORKING
# conn = psycopg2.connect(host="127.0.0.1", port=5432, database="CarParts", user="root", password="root")

conn.set_session(autocommit=True)
cur = conn.cursor()

cur.execute(
  """
  DROP TABLE IF EXISTS employee
  """)

cur.execute(
  """
  CREATE TABLE employee (id int PRIMARY KEY,
                         name varchar,
                         age int,
                         language varchar)
  """)
print("Created table employee")
cur.close()

# Take advantage of ordinary, transactional behavior for DMLs.

conn.set_session(autocommit=False)
cur = conn.cursor()

# Insert a row.

cur.execute("INSERT INTO employee (id, name, age, language) VALUES (%s, %s, %s, %s)",
            (1, 'John', 35, 'Python'))
print("Inserted (id, name, age, language) = (1, 'John', 35, 'Python')")

# Query the row.

cur.execute("SELECT name, age, language FROM employee WHERE id = 1")
row = cur.fetchone()
print("Query returned: %s, %s, %s" % (row[0], row[1], row[2]))

# Commit and close down.

conn.commit()
cur.close()
conn.close()