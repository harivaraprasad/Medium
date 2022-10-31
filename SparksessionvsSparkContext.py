from pyspark import SparkConf,SparkContext,SQLContext
from pyspark.sql import SparkSession
sc= SparkContext()
user1=spark.newSession()
user2=spark.newSession()
print('Main SparkContext  ',id(sc))
print('user1 SparkContext ',id(user1.sparkContext)) #id function returns the identity of an object
print('user2 SparkContext ',id(user1.sparkContext))

/*Main  SparkContext  140641551157328
user1 SparkContext  140641551157328
user2 SparkContext  140641551157328*/

print('sqlc User1: ',id(SQLContext(sparkContext=user1.sparkContext, sparkSession=user1)))
print('sqlc User2: ',id(SQLContext(sparkContext=user2.sparkContext, sparkSession=user2)))

/*sqlc User1:  140641428429776
sqlc User2:  140641428429392*/
  
  
df1=user1.createDataFrame([[1,2,3]],['col1'])
df1.createOrReplaceTempView('User1_table')
df2=user2.createDataFrame([[1,2,3]],['col1'])
df2.createOrReplaceTempView('User2_table')
df2.createOrReplaceGlobalTempView('Global_Table')
print('User1 Tables')
user1.sql('show tables').show()
print('User2 Tables')
user2.sql('show tables').show()
print('User1 Global Tables')
user2.sql('show tables from global_temp').show()
print('User2 Global Tables')
user1.sql('show tables from global_temp').show()

User1 Tables
+--------+-----------+-----------+
|database|  tableName|isTemporary|
+--------+-----------+-----------+
|        |user1_table|       true|
+--------+-----------+-----------+

User2 Tables
+--------+-----------+-----------+
|database|  tableName|isTemporary|
+--------+-----------+-----------+
|        |user2_table|       true|
+--------+-----------+-----------+

User1 Global Tables
+-----------+------------+-----------+
|   database|   tableName|isTemporary|
+-----------+------------+-----------+
|global_temp|global_table|       true|
|           | user2_table|       true|
+-----------+------------+-----------+

User2 Global Tables
+-----------+------------+-----------+
|   database|   tableName|isTemporary|
+-----------+------------+-----------+
|global_temp|global_table|       true|
|           | user1_table|       true|
+-----------+------------+-----------+
