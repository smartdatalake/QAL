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


