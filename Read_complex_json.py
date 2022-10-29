from pyspark.sql.functions import explode,expr,col,arrays_zip,count
from pyspark.sql.types import StructType,StructField,StringType,ArrayType


df=spark.read.option('multiline','true').option('schema',True).json('/FileStore/tables/Nested.json')

df1=df.withColumn('exploded',explode('nest'))

df2=df1.withColumn('release',col('exploded')['release']).withColumn('class',col('exploded')['universe']['class']). \
withColumn('subclass',col('exploded')['universe']['characteristics']['subclass']).withColumn('super1',col('exploded')['universe']['characteristics']['super'])

df3=df2.withColumn('zip',arrays_zip('subclass','super1')). \
withColumn('zip',explode('zip')).select('game','release','class',col('zip.subclass').alias('subclass'),col('zip.super1').alias('super'))

df_pivot=df3.groupBy('game').pivot('class',["Hunter", "Titan", "Warlock"]).agg(expr('first(super)'))
df_pivot.show()

class_count=df3.groupBy('class').pivot('game',['Destiny 1','Destiny 2']).agg(count('subclass'))
class_count.show()
