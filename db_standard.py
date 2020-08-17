import vertica_db_client
import sys 

#----------------- Function to measure query running time ---------------#
def get_time(cur):
	time = 0
	cur.execute("SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds FROM current_session;")
	if cur.rowcount != 0:
		exe_time = cur.fetchall()
		for i,t in enumerate(exe_time):
			time = float(t[0])
	return time

#----------------- Main queries -----------------------#
#Connection to vertica database
#This part should be changed when using vertica-python client to feet with it
db = vertica_db_client.connect("database=graphdb port=5433 user=dbadmin")
cur = db.cursor()

graph_file = sys.argv[1]
undirected = sys.argv[2].lower()

#Read data
print("Reading data ...\n")

time = 0
gf = open(graph_file, "r")
cur.stdin = gf
#Clear cach before each new execution
cur.execute("SELECT CLEAR_CACHES();")
cur.execute("DROP TABLE IF EXISTS E_s CASCADE;")
time= get_time(cur)

#Read the graph
cur.execute("CREATE TABLE E_s(i int NOT NULL,j int NOT NULL);")
time= time + get_time(cur)

cur.execute("COPY E_s FROM STDIN DELIMITER AS ' '", gf)
time= time + get_time(cur)

if (undirected=="undirected"):
	cur.execute("INSERT INTO E_s SELECT j,i FROM E_s;")
	time = time + get_time(cur)
cur.execute("COMMIT;")

print("Loading graph data took: " + str(time) + " seconds\n")

print("Triangle Enumeration ...")

#------------------- Triangle enumeration --------------------#
cur.execute("SELECT E1.i AS v1, E1.j AS v2, E2.j AS v3 FROM E_s E1 JOIN E_s E2 ON E1.j=E2.i \
 JOIN E_s E3 ON E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j ORDER BY v1,v2,v3;")
if cur.rowcount != 0:
	rows = cur.fetchall()
	print("Total of triangle count is " + str(cur.rowcount))
	time = time + get_time(cur)
	print("Total time of queries is " + str(time) + " seconds")
	with open("Triangle_enumeration.dat", 'w+') as triangles:
		for i, row in enumerate(rows):
			triangles.write(str(row)+'\n')
