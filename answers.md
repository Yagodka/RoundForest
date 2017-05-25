A - How do you make sure that there are no duplicates in the file?

    I will propose through the reduceByKey where 'key' is the composite key 
    for those columns for which we are looking for duplicates
    
A - We are interested in using full multi core CPU power. We will be running this 
  on machine with 500MB of RAM. How do you make sure that we are not using more 
  than that? How are you going to monitor the memory usage of your program?

    Spark is well suited for this))
    I limit the consumption of memory and cores through SparkConf:
          conf.set("spark.executor.memory", "512m")
          conf.set("spark.executor.cores", "4")


A - Please rate on a scale of 1 to 10 your skills in the following technologies, 
    please also mention any interesting hands-on experience you have:
    
    Back End micro services - 8
    Java -                  - 8
    Scala -                 - 9
    AWS -                   - 2 
    Docker -                - 6
    RabbitMQ -              - 8
    Kafka -                 - 6
    Akka -                  - 8 (actors, akka http, familiar with streams)
    Cassandra -             - 9
    MongoDB -               - 2 (Only read)
    Postgres -              - 7 (with the Postgres worked a little on the production through the Hibernate,
                                 but worked very hard with the MS SQL Server)
    Spark -                 - 6