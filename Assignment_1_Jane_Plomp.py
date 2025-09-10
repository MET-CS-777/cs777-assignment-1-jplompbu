from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
# Total fare/time less than $20/sec
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            #if (float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0 and float(p[11])/float(p[5])< 20):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("!!! Usage: main_task1 <file> <output1> <output2> ", file=sys.stderr)
        exit(-1)
    if os.path.isdir(sys.argv[2]):
        print(f"!!! The output folder '{sys.argv[2]}' already exists, delete and rerun")
        exit(-1)
    if os.path.isdir(sys.argv[3]):
        print(f"!!! The output folder '{sys.argv[3]}' already exists, delete and rerun")
        exit(-1)
    if os.path.isdir(sys.argv[4]):
        print(f"!!! The output folder '{sys.argv[4]}' already exists, delete and rerun")
        exit(-1)

    sc = SparkContext(appName="Assignment-1")
    spark = SparkSession.builder.getOrCreate()
    
    rdd = sc.textFile(sys.argv[1]).map(lambda x: x.split(','))

    # Filter for correct data
    taxilinesCorrected = rdd.filter(correctRows)

    df = spark.createDataFrame(taxilinesCorrected, ["Taxi", "Driver", "Pickup_dt", "dropoff_dt", "Time", "Distance", "Pickup_long", "Pickup_lat", "Dropoff_long", "Dropoff_lat", "Pay_type", "Fare", "Surcharge", "Tax", "Tip", "Tolls", "Total"])

    ################## Task 1 ##################

    # Group by Taxi
    # Count Distinct drivers
    # Convert to rdd to use the function...
    # Top 10, which will go through list in O(N) time and find the top number of drivers
    results_1 = df.groupBy("Taxi").agg(func.countDistinct("Driver")).rdd.top(10, key=lambda x: x[1])

    # results_1 ends up as a list, convert to rdd to use saveAsTextFile()
    results_1_rdd = sc.parallelize(results_1).map(tuple)
    # Save to output
    results_1_rdd.coalesce(1).saveAsTextFile(sys.argv[2])

    print("TASK 1: COMPLETE")


    ################## Task 2 ##################

    # Group by driver
    # Sum "Total" for profit AND sum "Time" for amount of time to get total $ and time for each driver
    results_2_0 = df.groupBy("Driver").agg(func.sum("Total"), func.sum("Time"))

    # Add a column called "Average_Money_per_Min" which is the sum of total $ dividied by (time divided by 60 to get min from seconds)
    # Covert to rdd to use the function...
    # Top 10, which will go through the list in O(N) time and find the top "Average_Money_per_Min"
    results_2 = results_2_0.withColumn("Average_Money_per_Min", col("sum(Total)") / (col("sum(Time)")/60)).rdd.top(10, key=lambda x: x[3])

    # results_2 ends up as a list, convert to rdd to use saveAsTextFile()
    results_2_rdd = sc.parallelize(results_2).map(tuple)
    # Save to output
    results_2_rdd.coalesce(1).saveAsTextFile(sys.argv[3])

    print("TASK 2: COMPLETE")

    ################## Task 3 ##################

    # Create an hour column, that pulls JUST the hour from the pickup datetime
    df2 = df.withColumn("Hour", func.hour(col("Pickup_dt")))

    # Group by Hour
    # Sum surcharge and distance
    results_3_0 = df2.groupBy("Hour").agg(func.sum("Surcharge"), func.sum("Distance"))

    # Create a column called "Avg_surcharge_per_mile" which is sum of surcharges divided by distance
    # Order the group so high Avg_surcharge_per_mile is at top
    results_3 = results_3_0.withColumn("Avg_surcharge_per_mile", col("sum(Surcharge)") / col("sum(Distance)")).orderBy("Avg_surcharge_per_mile", ascending=False)

    # Save to output
    results_3.rdd.map(tuple).coalesce(1).saveAsTextFile(sys.argv[4])

    print("TASK 3: COMPLETE")

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()