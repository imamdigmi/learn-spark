from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SimpleAppSpark').getOrCreate()
logFile = "/Users/imamdigmi/Code/spark/README.md"
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print(f'Lines with a: {numAs}, lines with b: {numBs}')

spark.stop()
