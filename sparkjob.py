from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import lead
import os

#  path of the json file
PROJECT_ID = "total-velocity-314603"
BUCKET_PATH = "gs://arifbucket-1"
BUCKET_NAME = BUCKET_PATH[BUCKET_PATH.rindex('/')+1:]
FILE_NAME = "covid_country_data"
data = f"{BUCKET_PATH}/covid-data/{FILE_NAME}.json"

# start a spark session and set up its configuration
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('sparkjob-to-bq') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', BUCKET_NAME)

# create a spark dataframe using the data in the csv
df = spark.read.json(data,
                multiLine='true' 
                # ,schema="dateFor DATE, China INT64, India INT64, Indonesia INT64, Italy INT64, United Kingdom INT64, United States INT64"
                )

# list for columns subtractions
colDiffs = []
# get only the country columns from the df columns list
countryCols = df.columns[1:]
# change the schema/type of the dateFor column from string to date
df = df.withColumn("dateFor", F.to_date("dateFor", "yyyy-MM-dd"))
# Window function spec to partition the df and sort it by Dates descending
# The entire dataset is partitioned (no argument passed to partitionBy) as there are no dates that show multiple times.
windowSpec = Window.partitionBy().orderBy(F.col('dateFor').desc())
# for each country column in the columns list
for country in countryCols:
    # add a new column, countrynameDiff, to the df containing the same numbers but shifted up by one using "lead"
    # E.g.: if a column X contains the numbers [1, 2, 3], applying the "lead" window function, with 1 as argument, will
    # shift everything up by 1 and the new XDiff column will contain [2, 3, none]
    df = df.withColumn(f'{country}Diff', lead(country, 1).over(windowSpec))
    # add the subtraction to the list with the condition that if the calculated value is lower than 0, then save 0
    # this saves the subtraction formula in the list, not the result of the subtraction.
    # the header of the subtraction result column will be the same as the "country" by applying "alias"
    colDiffs.append(F.when((df[country] - df[f'{country}Diff']) < 0, 0)
                    .otherwise(df[country] - df[f'{country}Diff']).alias(country))
# select the dateFor column and calculate the subtractions in the df, returning a new dataframe with the results
result = df.select('dateFor', *colDiffs).fillna(0)

result.write.format("bigquery") \
    .option('table', f'{PROJECT_ID}.mydataset.{FILE_NAME}') \
    .mode('overwrite') \
    .save()
