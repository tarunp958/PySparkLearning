from pyspark.sql import *



if __name__ == "__main__":
    print("Tarun")

    spark = SparkSession.builder.appName("Hello Spark").master("local[2]").getOrCreate()

    data_list = [("Tarun", 26),
                 ("Vipul", 28),
                 ("Arun",27)]
    
    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()
