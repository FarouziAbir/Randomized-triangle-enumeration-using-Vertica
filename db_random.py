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
if __name__ == "__main__":

	#Connection to vertica database
	db = vertica_db_client.connect("database=graphdb port=5433 user=dbadmin")
	cur = db.cursor()

	graph_file = sys.argv[1]
	tripletfile = sys.argv[2]
	undirected = sys.argv[3].lower()

	time = 0

	#Read graph data
	print("Reading data ...")
	gf = open(graph_file, "r")
	cur.stdin = gf
	#Clearing the cache before each new execution
	cur.execute("SELECT CLEAR_CACHES();")
	cur.execute("DROP TABLE IF EXISTS E_s CASCADE;")
	time = time + get_time(cur)
	cur.execute("CREATE TABLE E_s(i int NOT NULL,j int NOT NULL) PARTITION BY i;")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_super(i ENCODING RLE, j ENCODING RLE) AS SELECT i,j FROM E_s ORDER BY i,j SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("COPY E_s FROM STDIN DELIMITER AS ' '", gf)
	time = time + get_time(cur)

	if (undirected=="undirected"):
		cur.execute("INSERT INTO E_s SELECT j,i FROM E_s;")
		time = time + get_time(cur)

	print("Loading graph Data set took: " + str(time) + " seconds ")

	#Random Solution
	tf = open(tripletfile, "r")
	cur.stdin = tf
	cur.execute("DROP TABLE IF EXISTS triplet CASCADE;")
	time = time + get_time(cur)
	cur.execute("CREATE TABLE triplet(machine int,color1 int,color2 int,color3 int);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION triplet_super(machine, color1, color2, color3) AS SELECT machine, color1,color2,color3 FROM triplet ORDER BY machine UNSEGMENTED ALL NODES;")
	time = time + get_time(cur)
	cur.execute("COPY triplet FROM STDIN DELIMITER AS ',';")
	time = time + get_time(cur)

	#Assign random color to V
	cur.execute("DROP TABLE IF EXISTS V_s CASCADE;")
	time = time + get_time(cur)
	cur.execute("CREATE TABLE V_s(i int,color int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION V_s_super(i, color ENCODING RLE) AS SELECT i, color FROM V_s ORDER BY i SEGMENTED BY hash(i) ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO V_s SELECT i,randomint(2)+1 FROM (SELECT DISTINCT i FROM E_s UNION SELECT DISTINCT j FROM E_s)V;")
	time = time + get_time(cur)
	cur.execute("COMMIT;")

	#Color edges based on colord vertices, edges are repartitioned
	cur.execute("DROP TABLE IF EXISTS E_s_proxy CASCADE;")
	time = time + get_time(cur)
	cur.execute("CREATE TABLE E_s_proxy(i_color int NOT NULL,j_color int NOT NULL,i int NOt NULL,j int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_proxy_super(i_color ENCODING RLE, j_color ENCODING RLE, i, j) AS SELECT i_color,j_color,i,j FROM E_s_proxy ORDER BY i,j SEGMENTED BY hash(i_color,j_color) ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)
	cur.execute("INSERT INTO E_s_proxy SELECT Vi.color, Vj.color,E.i,E.j FROM E_s E JOIN V_s Vi ON E.i=Vi.i JOIN V_s Vj ON E.j=Vj.i;")
	time = time + get_time(cur)
	cur.execute("COMMIT;")

	#Send from proxy to local
	cur.execute("DROP TABLE IF EXISTS E_s_local CASCADE;")
	time = time + get_time(cur)
	cur.execute("CREATE TABLE E_s_local(machine int NOT NULL,i int NOT NULL,j int NOT NULL,i_color int NOT NULL,j_color int NOT NULL);")
	time = time + get_time(cur)
	cur.execute("CREATE PROJECTION E_s_local_super(machine ENCODING RLE, i, j, i_color ENCODING RLE, j_color ENCODING RLE) AS SELECT machine, i,j, i_color,j_color FROM E_s_local ORDER BY i,j SEGMENTED BY (machine*4294967295//8) ALL NODES OFFSET 0 KSAFE 1;")
	time = time + get_time(cur)

	cur.execute("INSERT INTO E_s_local SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge1 ON E.i_color=edge1.color1 and E.j_color=edge1.color2 and E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge2 ON E.i_color=edge2.color2 and E.j_color=edge2.color3 and E.i<E.j UNION SELECT machine, i, j, i_color, j_color FROM E_s_proxy E JOIN triplet edge3 ON E.i_color=edge3.color3 AND E.j_color=edge3.color1 and E.i>E.j;")
	time = time + get_time(cur)
	cur.execute("COMMIT;")

	print("Triangle enumeration ...")

	#triangles, but locally on each machine
	time1 = 0
	tr1=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=1 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0001' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr1= cur.rowcount
		print("Triangle count on machine 1: " + str(tr1))
		time1 = get_time(cur)
		print("Execution time on machine 1: " + str(time1))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	time2=0
	tr2=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=1 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0002' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr2= cur.rowcount
		print("Triangle count on machine 2: " + str(tr2))
		time2 = get_time(cur)
                print("Execution time on machine 2: " + str(time2))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	time3=0
	tr3=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=2 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0003' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr3= cur.rowcount
		print("Triangle count on machine 3: " + str(tr3))
		time3 = get_time(cur)
                print("Execution time on machine 3: " + str(time3))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	time4=0
	tr4=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=1 AND E1.j_color=2 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0004' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr4= cur.rowcount
		print("Triangle count on machine 4: " + str(tr4))
		time4 = get_time(cur)
                print("Execution time on machine 4: " + str(time4))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	time5=0
	tr5=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=1 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0005' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr5= cur.rowcount
		print("Triangle count on machine 5: " + str(tr5))
		time5 = get_time(cur)
                print("Execution time on machine 5: " + str(time5))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	time6=0
	tr6=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=1 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0006' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr6= cur.rowcount
		print("Triangle count on machine 6: " + str(tr6))
		time6 = get_time(cur)
                print("Execution time on machine 6: " + str(time6))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	time7=0
	tr7=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=2 AND E2.j_color=1 AND local_node_name()='v_graphdb_node0007' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr7= cur.rowcount
		print("Triangle count on machine 7: " + str(tr7))
		time7 = get_time(cur)
                print("Execution time on machine 7: " + str(time7))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	time8=0
	tr8=0
	cur.execute("SELECT E1.machine, E1.i AS v1, E1.j AS v2, E2.j AS v3  FROM E_s_local E1 JOIN E_s_local E2 ON E1.machine=E2.machine AND E1.j=E2.i JOIN E_s_local E3 ON E2.machine=E3.machine AND E2.j=E3.i WHERE E1.i<E1.j AND E2.i<E2.j AND E1.i=E3.j AND E1.i_color=2 AND E1.j_color=2 AND E2.j_color=2 AND local_node_name()='v_graphdb_node0008' ORDER BY v1,v2,v3;")
	if cur.rowcount != 0:
		rows = cur.fetchall()
		tr8= cur.rowcount
		print("Triangle count on machine 8: " + str(tr8))
		time8 = get_time(cur)
                print("Execution time on machine 8: " + str(time8))
	#	with open("Triangles_"+graph_file.split("/")[2], 'a+') as triangles:
	#		for i, row in enumerate(rows):
	#			triangles.write(str(row)+'\n')

	print("\n")
	time1 = (time1+time2+time3+time4+time5+time6+time7+time8)/8
	time = time + time1
	triangle = tr1+tr2+tr3+tr4+tr5+tr6+tr7+tr8
	print("Triangle enumeration on all machine took: " + str(time1) + " seconds" )
	print("Total time is: " +  str(time) + " seconds")
	print("Total of triangle count is " + str(triangle))

