# logging setting to keep track of data set logs 

import logging 
from datetime import datetime 
import os 



# created a check here just in case if the logs folder is not present on my partner's end
os.makedirs("logs", exist_ok = True)





# Creating a timestamp for each log file 

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"logs/loadingdataset_{timestamp}.log"

logging.basicConfig(filename = log_file, level = logging.INFO, format = "%(asctime)s - %(levelname)s - %(message)s")

logging.info("== New session started ==")



# spark session 



from pyspark.sql import SparkSession #importing spark session 

spark = SparkSession.builder.appName("LoadingDataSet").getOrCreate() # generating spark session



# Loading both the datasets

df_customer = spark.read.csv("data/raw/CS236_Project_Fall2025_Datasets/customer-reservations.csv", header = True, inferSchema = True)

df_hotel = spark.read.csv("data/raw/CS236_Project_Fall2025_Datasets/hotel-booking.csv", header = True, inferSchema = True)




# displaying each data frame 

df_customer.show() 

df_hotel.show()


# printing each schema 

df_customer.printSchema()
df_hotel.printSchema()










# https://www.youtube.com/watch?v=de9626esw40

# https://www.youtube.com/watch?v=jWnTiN0tuvo

# https://www.sparkplayground.com/blog/inferschema-in-pyspark

# https://polarpersonal.medium.com/writing-pyspark-logs-in-apache-spark-and-databricks-8590c28d1d51 



