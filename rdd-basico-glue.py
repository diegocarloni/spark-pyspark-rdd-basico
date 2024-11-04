from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Inicializa o GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Cria um DataFrame a partir de uma lista de números
numeros = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(numeros)
df = rdd.map(lambda x: Row(numero=x)).toDF()

df.show()