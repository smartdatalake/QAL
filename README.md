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

1) Install Git, Java, Scala, and SBT.
2) Clone or fork the project.
3) The executable jar file and configuration files are in the folder /executable; you can skip steps 4 and 5.
4) To make executable jar file, package and assembly the source project with SBT.
5) Go to project_directory/target/scala-2.11, and get executable SDL_QAL_API.jar



###### Setup QAL REST API:

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


