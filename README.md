# SparkAssignment

### Build project with SBT

Download the git repository  

```
git clone "https://github.com/Beubz/SparkAssignment.git"
```

In the root directory you can package the project 

```
sbt package
```

### Run project

```
./spark-1.5.2/bin/spark-submit assignment_2.10-1.0.jar 2>/dev/null
```

###### What is the crime that happens the most in Sacramento ?

![alt tag](https://github.com/beubz/SparkAssignment/img/RDD1.png)

###### Give the 3 days with the highest crime count

###### Calculate the average of each crime per day

### Get results

To get the CSV file after exporting it :

```
mv csvFile/part-00000 result.csv
```