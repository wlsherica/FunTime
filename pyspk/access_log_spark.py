#operate access_log.txt from hdfs in pyspark

lines = sc.textFile("/user/erica/access_log.txt")
parts = lines.map(lambda l: l.split(","))
log = parts.map(lambda p: Row(host=p[0], client_identd =p[1], user_id=p[2], stime=p[3], method=p[4], endpoint=p[5], protocol=p[6], response_code=p[7], last_page=p[8], agent=p[9], browser=p[10], os=p[11]))

# Apply the schema to the RDD.
log_df = sqlContext.createDataFrame(log)

# Register the DataFrame as a table.
log_df.registerTempTable("log")

# save 
log_df.save('/user/erica/access_log_2')

#reload
df2 = sqlContext.read.load('/user/erica/access_log_2')

# SQL can be run over DataFrames that have been registered as a table.
results = sqlContext.sql("SELECT distinct browser FROM log ")

