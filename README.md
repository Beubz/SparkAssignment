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

![Question 1 - RDD](https://github.com/Beubz/SparkAssignment/blob/master/img/RDD1.png)
![Question 1 - DF](https://github.com/Beubz/SparkAssignment/blob/master/img/DF1.png)

We can see that the crime that happens the most is 10851(A)VC TAKE VEH W/O OWNER and it happened 653 times


###### Give the 3 days with the highest crime count

![Question 2 - RDD](https://github.com/Beubz/SparkAssignment/blob/master/img/RDD2.png)
![Question 2 - DF](https://github.com/Beubz/SparkAssignment/blob/master/img/DF2.png)

The 11th, 18th and 17th weren't really good days for Sacramento..


###### Calculate the average of each crime per day

![Question 3 - RDD](https://github.com/Beubz/SparkAssignment/blob/master/img/RDD3.png)
![Question 3 - DF](https://github.com/Beubz/SparkAssignment/blob/master/img/DF3.png)

To see all the result, you can export it in a CSV file

### Get results

To get the CSV file after exporting it :

```
mv csvFile/part-00000 result.csv
```