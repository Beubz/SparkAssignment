# SparkAssignment

### Build projects with SBT

Download the git repository  

```
git clone "https://github.com/Beubz/mapReduceAssigment1.git"
```

In the root directory you can package the project 


```
sbt package
```

Now you need to put the JAR on the server

```
scp target/scala-2.10/assignment_2.10-1.0.jar IP_DU_SERVEUR
```

On the serveur, you can execute the JAR file as follows

```
./spark-1.5.2/bin/spark-submit assignment_2.10-1.0.jar 2>/dev/null
```

To get the CSV file after exporting it :

```
mv csvFile/part-00000 result.csv
```