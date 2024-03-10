from tkinter import *
from tkinter import ttk
import tkinter.font as font

import time 

from pyspark import *
from pyspark.sql import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sqlsum
import pyspark.sql.functions as f

from graphframes.lib import AggregateMessages as AM
from graphframes import *

# https://spark-packages.org/package/graphframes/graphframes
# pip install graphframes 
# pip install pyspark

# Running the program
# spark-submit --master local[*] --deploy-mode client --jars graphframes-0.8.2-spark3.2-s_2.12.jar,neo4j-connector-apache-spark_2.12-5.0.0_for_spark_3.jar flightPlanner.py

# This is a way to make sure that we don't overwhelm the program with to many path finding submissions
g_calculating_shortest_path = False 

# Find the shortest path between airports using apache spark
def shortestPath(_source_id, _dest_id):
    airportStops.set([])
    global g_calculating_shortest_path
    if g_calculating_shortest_path == False:
        g_calculating_shortest_path = True
        print("Path Finding!")
        start = time.time()
        start_id_string = "id = '{}'".format(_source_id.get())
        end_id_string = "id = '{}'".format(_dest_id.get())
        country = None 
        # Check if the flight is domestic, or goes international
        source_country = airport_id_to_country_name_hash[_source_id.get()]
        dest_country = airport_id_to_country_name_hash[_dest_id.get()]

        airport_graph = None 
        # Domestic flight, easy
        if(source_country == dest_country):
            airport_graph = country_graph_frame_hash[source_country]

            paths = airport_graph.bfs(start_id_string, end_id_string)
            airport_routes_list = paths.collect()
            airport_names = []
            for airport_route in airport_routes_list:
                airport_route_hash = airport_route.asDict()
                airport_hash_keys = list(airport_route_hash.keys())
                # We only want to save the vertices 
                airport_names.append(airport_route_hash['from'].asDict()['name'])
                for key in airport_hash_keys:
                    if key[0] == 'v':
                        airport_names.append(airport_route_hash[key].asDict()['name'])
                airport_names.append(airport_route_hash['to'].asDict()['name'])
                break

            airportStops.set(airport_names)
            end = time.time()
            print(end-start)
            g_calculating_shortest_path = False
            print("FINISHED")
            return 
        
        # International flight, more complicated
        airport_graph = country_graph_frame_hash[source_country]
        source_international_airport_id = country_to_international_airport[source_country]
        dest_international_airport_id = country_to_international_airport[dest_country]

        # Path from starting city to international airport 
        end_id_string = "id = '{}'".format(source_international_airport_id)
        paths_one = airport_graph.bfs(start_id_string, end_id_string)

        # Path from the starting city's international airport to the ending city's international 
        # airport.
        airport_graph = international_graph_frame
        next_end_id_string = "id = '{}'".format(dest_international_airport_id)
        paths_two = airport_graph.bfs(end_id_string, next_end_id_string)

        # Path from the ending city's international airport to the ending city
        dest_id_string = "id = '{}'".format(_dest_id.get())
        airport_graph = country_graph_frame_hash[dest_country]
        paths_three = airport_graph.bfs(next_end_id_string, dest_id_string)

        # First set of paths
        airport_routes_list = paths_one.collect()
        airport_names = []
        for airport_route in airport_routes_list:
            airport_route_hash = airport_route.asDict()
            airport_hash_keys = list(airport_route_hash.keys())
            # We only want to save the vertices 
            airport_names.append(airport_route_hash['from'].asDict()['name'])
            for key in airport_hash_keys:
                if key[0] == 'v':
                    airport_names.append(airport_route_hash[key].asDict()['name'])
            airport_names.append(airport_route_hash['to'].asDict()['name'])
            break

        # Second set of paths
        airport_routes_list = paths_two.collect()
        for airport_route in airport_routes_list:
            airport_route_hash = airport_route.asDict()
            airport_hash_keys = list(airport_route_hash.keys())
            # We only want to save the vertices 
            #airport_names.append(airport_route_hash['from'].asDict()['name'])
            for key in airport_hash_keys:
                if key[0] == 'v':
                    airport_names.append(airport_route_hash[key].asDict()['name'])
            airport_names.append(airport_route_hash['to'].asDict()['name'])
            break

        # Third set of paths
        airport_routes_list = paths_three.collect()
        for airport_route in airport_routes_list:
            airport_route_hash = airport_route.asDict()
            airport_hash_keys = list(airport_route_hash.keys())
            # We only want to save the vertices 
            #airport_names.append(airport_route_hash['from'].asDict()['name'])
            for key in airport_hash_keys:
                if key[0] == 'v':
                    airport_names.append(airport_route_hash[key].asDict()['name'])
            airport_names.append(airport_route_hash['to'].asDict()['name'])
            break

        airportStops.set(airport_names)
        end = time.time()
        print(end-start)
        print("FINISHED")
        g_calculating_shortest_path = False
        

# Get the import contexts
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sql_context = SQLContext(sc)

#Load Neo4j Airports
df_airport = spark.read.format("org.neo4j.spark.DataSource") \
.option("url", "bolt://localhost:7687") \
.option("authentication.basic.username", "neo4j") \
.option("authentication.basic.password", "1234") \
.option("labels", "Airport") \
.load()

# Load Neo4j Airport Relationships
df_airport_relationships = spark.read.format("org.neo4j.spark.DataSource") \
    .option("url", "bolt://localhost:7687") \
    .option("authentication.basic.username", "neo4j") \
    .option("authentication.basic.password", "1234") \
    .option("relationship", "HAS_ROUTE") \
    .option("relationship.source.labels", "Airport") \
    .option("relationship.target.labels", "Airport") \
    .load()

airport_row_list = df_airport.collect()
airport_relationships_list = df_airport_relationships.collect()

# To optimize the graph traversal we are dividing up the graphs into airports within a nation,
# and airports that connect to other nations globally. This should be an easy way to break up the 
# graph to reduce computations.

# This is used for international connections
country_connector_edge_hash = {}
# This is used for points that are inside of a country
intra_country_edge_hash = {}

domestic_edges = 0 
international_edges = 0
domestic_airport_hash_test = {}
international_airport_hash_test = {}
# Loop over each airport connection connecting them together
for airport_relation_i, airport_relation, in enumerate(airport_relationships_list):
    airport_relationships_row_dict = airport_relationships_list[airport_relation_i].asDict()
    source_airport_id = airport_relationships_row_dict['source.airport_id']
    source_country = airport_relationships_row_dict['source.country']

    dest_airport_id = airport_relationships_row_dict['target.airport_id']
    dest_country = airport_relationships_row_dict['target.country']

    route_tuple = (source_airport_id, dest_airport_id) 
    
    # Check if this flight is international
    if source_country != dest_country:
        international_edge_list = []
        if source_country not in country_connector_edge_hash:
            country_connector_edge_hash[source_country] = international_edge_list
        else:
            international_edge_list = country_connector_edge_hash[source_country]

        international_edge_list.append(route_tuple)

        international_airport_hash_test[source_airport_id] = True
        international_edges += 1
    else:
        edge_list = []
        if source_country not in intra_country_edge_hash:
            intra_country_edge_hash[source_country] = edge_list
        else:
            edge_list = intra_country_edge_hash[source_country]

        edge_list.append(route_tuple)

        domestic_airport_hash_test[source_airport_id] = True
        domestic_edges += 1

country_to_international_airport = {}

# This converts the country's id into a given name
airport_id_to_country_name_hash = {}

# This is used for international connections
country_connector_vertex_hash = {}
# This is used for points that are inside of a country
intra_country_vertex_hash = {}
# Loop over each airport storing it in the correct format
for airport_i, airport, in enumerate(airport_row_list):
    airport_row_dict = airport_row_list[airport_i].asDict()
    airport_id = airport_row_dict['airport_id']
    airport_name = airport_row_dict['airport_name']
    country = airport_row_dict['country']

    airport_id_to_country_name_hash[airport_id] = country 

    vertex_tuple = (airport_id, airport_name, country)

    country_vertex_list = []
    # Check if this nation already exists
    if country not in intra_country_vertex_hash:
        intra_country_vertex_hash[country] = country_vertex_list
    else:
        country_vertex_list = intra_country_vertex_hash[country]

    country_vertex_list.append(vertex_tuple)

    # Add this airport to the international vertex list if it has international edges
    if airport_id in international_airport_hash_test:
        # Used to path out of the country
        country_to_international_airport[country] = airport_id

        international_vertex_list = []
        # Check if this nation already exists
        if country not in country_connector_vertex_hash:
            country_connector_vertex_hash[country] = international_vertex_list
        else:
            international_vertex_list = country_connector_vertex_hash[country]

        international_vertex_list.append(vertex_tuple)

# Stores every country's graph frame
country_graph_frame_hash = {}
# For every domestic flight we need to store a GraphFrame
for country_i, country in enumerate(list(intra_country_vertex_hash.keys())):
    #print(country + " " + str(country_i))
    if country in intra_country_edge_hash:
        country_edge_list = intra_country_edge_hash[country]
        country_vertex_list = intra_country_vertex_hash[country]

        edge_df = sql_context.createDataFrame(country_edge_list, ["src", "dst"])
        vertex_df = sql_context.createDataFrame(country_vertex_list, ["id", "name", "country"])
        # Create and cache the entire airport graph
        airport_graph = GraphFrame(vertex_df, edge_df)
        airport_graph.cache()

        country_graph_frame_hash[country] = airport_graph

        #print(len(country_edge_list))
        #print(len(country_vertex_list))
    else:
        pass
        #print(len(intra_country_vertex_hash[country]))

international_graph_frame = None 
# Create the international graph frame 
vertex_list = []
edge_list = []
for country_i, country in enumerate(list(country_connector_vertex_hash.keys())):
    if country in country_connector_edge_hash:
        country_edge_list = country_connector_edge_hash[country]
        country_vertex_list = country_connector_vertex_hash[country]

        edge_list.extend(country_edge_list)
        vertex_list.extend(country_vertex_list)

edge_df = sql_context.createDataFrame(edge_list, ["src", "dst"])
vertex_df = sql_context.createDataFrame(vertex_list, ["id", "name", "country"])
international_graph_frame = GraphFrame(vertex_df, edge_df)
international_graph_frame.cache()
  
# Create the Tkinter window
root = Tk()
root.title("Airplane Flight Planner")
root.minsize(1280, 720)
root.resizable(False, False)

myFont = font.Font(size=30)

# Set up the frame to position the elements
mainframe = ttk.Frame(root)
mainframe.grid(column=0, row=0, sticky=(N,W,E,S))
root.columnconfigure(0, weight=1)
root.rowconfigure(0, weight=1)

# Source Airport 
ttk.Label(mainframe, text="Source Airport ID", font=("Calibri, 20"), borderwidth=1, relief="solid").grid(column=1, row=1, sticky=W, padx=10, pady=10)
# Airport Name Entry Box 
source_airport_name = StringVar()
source_airport_name_entry = ttk.Entry(mainframe, textvariable=source_airport_name, font=("Calibri, 20"), width=17).grid(column=1, row=2, sticky=W, padx = 10, pady=10)

# Destination Airport 
ttk.Label(mainframe, text="Destination Airport ID", font=("Calibri, 20"), borderwidth=1, relief="solid").grid(column=2, row=1, sticky=W)
# Airport Name Entry Box 
dest_airport_name = StringVar()
dest_airport_name_entry = ttk.Entry(mainframe, textvariable=dest_airport_name, font=("Calibri, 20"), width=19).grid(column=2, row=2, sticky=W, padx = 10, pady=10)

# Shortest Path Calculation Button
style = ttk.Style()
style.configure('buttonStyle.TButton', font=('Calibri', 20))
shortest_path_button = ttk.Button(mainframe, text="Calculate Shortest Path", command=lambda: shortestPath(source_airport_name, dest_airport_name), style='buttonStyle.TButton').grid(column=3, row=2, sticky=W)

# Display the shortest path nodes
airportStops = StringVar(value=[])
shortest_path_list = Listbox(mainframe, listvariable=airportStops, height=30, width=40).grid(column=3, row=3, sticky=W)

root.mainloop()