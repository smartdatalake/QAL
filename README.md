The QAL service is available via a REST API receiving approximate queries in JSON format and returning the approximated results in JSON format. By default, the QAL service listens on port 4545, and it is configurable at installation phase. The input data must exist in RAW as a materialized view, and it must be accessible via the user’s Proteus JDBC connector. The input parameters and API functions are as follows:

/alive: it checks the status of the QAL service. It has no input parameter.

/QAL: it receives the approximate query annotated with a user-defined confidence interval. If the query does not contain an approximation rate, then QAL executes the exact version of it. The query string is sent via the query parameter inside the request. As the QAL receives a query, it contacts the Proteus server to fetch required columns from the target tables, so the tables must be accessible from the Proteus instance. The names and definitions of input parameters are: 

   ● query: it contains the string of the approximate query.

/changeWRSize: QAL stores synopses in the warehouse to use them again. This request sets the size of the warehouse in MB. Default is 1000 MB.

   ● quota: it indicates the new warehouse size in MB.

/flushSample: QAL stores synopses in the warehouse to reuse them in next queries execution. This request flushes the warehouse and removes synopses. No input parameter is required. No input parameter is required.

/removeTable: QAL stores the input table to avoid re-accessing the original data from Proteus. Calling this request removes buffered tables. No input parameter is required.

/changeProteus: To execute a query, QAL requires a running Proteus server to fetch the proper data. Input parameters are:

   ● url: it is an address to a running Proteus server
   
   ● username: it is a username to the Proteus server

   ● pass: it is a password for the Proteus username
   
QAL is implemented in Scala v.2.11.12, and it runs on top of Apache Spark v.2.4.3. The source code is available in SmartDataLake Github repository under the directory named QAL. All functionality is under a single project and has been built and tested using Java JDK v.1.8 and SBT v.1.3.13. The final component is available as a Docker image hosting Spark instance and as a stand-alone jar file. First, we describe how to fetch the source code, install required dependencies, prepare the configuration files, and submit the stand-alone jar file to a running Spark cluster for execution. Then, we present the instructions for running the QAL service inside a Spark Docker image.

###### Create executable JAR file:

The source code is available on the GitHub repository of SmartDataLake. To make a stand-alone jar file, the following steps are required:
1) Install Git, Java, Scala, and SBT.
2) Clone or download the QAL project from the source code.
3) Package and assembly the source project with SBT.
4) Go to project_directory/target/scala-2.11, and get the executable jar file named SDL_QAL_API.jar
5) Create four empty folders in the same directory of the jar file, named data_csv, data_parquet, materializedSketches, and materializedSynopses.
6) Copy avatica-1.13.0.jar which is a library for Proteus JDBC connector, to the same folder of SDL_QAL_API.jar.
7) If the service is going to be installed without a REST APIs endpoint, create a text file named log.txt that each line of the file is an approximate query.
8) Create a JSON file named QALconf.txt in the same folder of SDL_QAL_API.jar which contains configurations as shown below:
{
“fractionInitialize” : initial value for sampling rate,
“maxSpace” : size of the warehouse in megaByte,
“windowSize” : size of the sliding window for looking into past queries. It cannot be changed after the installation,
“REST” : let be true to enable REST API; otherwise, QAL reads queries from log.txt,
“port” : port for listening to REST APIs. It cannot be changed after the installation,
"parentDir"; : directory to store synopses and metadatas,
“ProteusJDBC_URL”: set default address to the Proteus server,
“ProteusUsername” : set Proteus default username,
“ProteusPassword”: set password for the username,
}
9) Start a Spark cluster and Proteus server having JDBC connection.
10) In command line do:
path_to_spark-submit --class mainSDL --driver-class-path path_to_avatica_jar_file --master spark_cluster_IP --executor-memory XXg --driver-memory XXg SDL_QAL_API.jar
11) QAL REST APIs are listening on the port configured in QALconf.txt; Check the service by sending GET request IP:port/alive.
12) Send your approximate query as a GET request to localhost:port/QAL.

###### Docker image installation

The QAL service requires a running Spark cluster to execute the stand-alone jar file. To make it easier for the user, we provide a Docker image hosting a Spark cluster and inject the QAL service inside this image. After executing and deploying the image, the QAL service exposed over a desired port, will be ready as a REST API. To prepare the Docker image, the following steps should be performed:
1) Generate the stand-alone jar file named SDL_QAL_API.jar
2) In the same folder, put the jar file avatica-1.13.0.jar, empty buffer.tmp, and conf.txt
3) Set QALconf.txt as below:
{
“fractionInitialize” : 0.1,
“maxSpace” : 1000,
“windowSize” : 20,
“REST” : true,
“port” : 4545,
"parentDir" : “”,
“ProteusJDBC_URL”: set default address to the Proteus server,
“ProteusUsername” : set Proteus default username,
“ProteusPassword”: set password for the username,
}
4) Make a plain file named Dockerfile containing the code below:
FROM gradiant/spark:latest
RUN mkdir /opt/spark-2.4.4-bin-hadoop2.7/local/data_csv
RUN mkdir /opt/spark-2.4.4-bin-hadoop2.7/local/materializedSketches
RUN mkdir /opt/spark-2.4.4-bin-hadoop2.7/local/data_parquet
RUN mkdir /opt/spark-2.4.4-bin-hadoop2.7/local/materializedSynopses
RUN touch /opt/spark-2.4.4-bin-hadoop2.7/local/log.txt
COPY SDL_QAL_API.jar /opt/spark-2.4.4-bin-hadoop2.7/local/
COPY QALconf.txt /opt/spark-2.4.4-bin-hadoop2.7/local/
COPY avatica-1.13.0.jar /opt/spark-2.4.4-bin-hadoop2.7/local/
EXPOSE 4545
CMD ["spark-submit","--class"," mains.SDL", "--master",";spark://spark-master:7077",
"SDL_QAL_API.jar"]

5) Open a command line in the same directory and run:
docker build -t qal:1.0
6) Deploy the image with:
docker run -d -p 127.0.0.1:4545:4545/tcp qal:1.0
7) Start a Proteus instance and add its credentials via localhost:4545/changeProteus. The QAL REST API is listening on port 4545. Check the service by sending a GET request to localhost:4545/alive



###### Docker image installation

1) The system requires a running Spark cluster and Proteus JDBC connection. 
2) Copy avatica-1.13.0.jar file to the same folder of SDL_QAL_API.jar
3) Create QALconf.txt in the same folder of SDL_QAL_API.jar, which contains configurations. Please, check
   /executable/QALconf.txt as an example.

QALconf.txt must contain the json below:

{

    "fractionInitialize" : 0.1,

    "maxSpace" : warehouse_size_in_megabyte,

    "windowSize" : sliding_window_size,

    "REST" : true,

    "port" : prot_number,

    "parentDir" : "path_to_store_data",

    "ProteusJDBC_URL" : "address",

    "ProteusUsername" : "username",

    "ProteusPassword" : "pass"
}
4) In command line do: path_to_spark-submit --class mainSDL --driver-class-path path_to_avatica_jar_file --master spark_cluster_IP --executor-memory 12g --driver-memory 12g SDL_QAL_API.jar
5) QAL REST API is listening on the port configured in QALconf.txt
6) Check the REST API by sending GET request IP:port/alive
7) To change Proteus server address and credential, send GET request IP:port/changeProteus with the query params below:

{

'url' : "X", // new address

'username' : "Y", // new username

'pass' : "Z"    // new password

}

8) Send your approximate query as a GET request to IP:port/QAL with the query params below:

{
'query' : "your_query" 
}


