from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

# Inicializa o GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Cria um DataFrame a partir de uma lista de números
numeros = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(numeros)
df = rdd.map(lambda x: Row(numero=x)).toDF()

# Exibir o DataFrame
df.show()

# Ações
# Ver os primeiros 5 elementos do DataFrame
df.show(5)

# Operações aritméticas
df.count()       # Contar elementos
df.agg(F.sum("numero")).show()       # Soma
df.agg(F.mean("numero")).show()      # Média
df.agg(F.stddev("numero")).show()    # Desvio padrão
df.agg(F.max("numero")).show()       # Valor máximo
df.agg(F.min("numero")).show()       # Valor mínimo

# Transformações e filtros
# Filtrar números maiores que 2
filtro = df.filter(df["numero"] > 2)
filtro.show()

# Amostra aleatória com reposição
amostra = df.sample(withReplacement=True, fraction=0.5, seed=1)
amostra.show()

# Mapear os elementos do DataFrame
mapa = df.select((F.col("numero") * 2).alias("numero_duplicado"))
mapa.show()

# União de DataFrames
numeros2 = [6, 7, 8, 9, 10]
df2 = sc.parallelize(numeros2).map(lambda x: Row(numero=x)).toDF()
uniao = df.union(df2).distinct()
uniao.show()

# Interseção de DataFrames
interseccao = df.join(df2, on="numero", how="inner")
interseccao.show()

# Subtração de DataFrames
subtracao = df.join(df2, on="numero", how="left_anti")
subtracao.show()

# Produto cartesiano de DataFrames
cartesiano = df.crossJoin(df2)
cartesiano.show()

# Exemplo de chave-valor usando DataFrames
compras = sc.parallelize([(1, 200), (2, 300), (3, 120), (4, 250), (5, 78)]).toDF(["id", "valor"])

# Selecionar apenas as chaves
chaves = compras.select("id")
chaves.show()

# Selecionar apenas os valores
valores = compras.select("valor")
valores.show()

# Contar elementos por chave
contagem = compras.groupBy("id").count()
contagem.show()

# Aplicar uma função a cada valor de cada chave
soma = compras.withColumn("valor", compras["valor"] + 1)
soma.show()

# Criar um DataFrame com débitos
debitos = sc.parallelize([(1, 20), (2, 300)]).toDF(["id", "debito"])

# Join entre compras e débitos
resultado = compras.join(debitos, on="id", how="inner")
resultado.show()

# Subtração de DataFrames com chave-valor
semdebito = compras.join(debitos, on="id", how="left_anti")
semdebito.show()