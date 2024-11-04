#Descricao: Exemplo de criacao de um RDD basico no pyspark localmente. Nesse caso, o sc (sparkContext) é criado automaticamente, assim como a sessao do spark (sparkSession).

#criando um rdd
numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

#Acoes

#Ver os primeiros 5 elementos do rdd
numeros.take(5)

#Ver os ultimos 5 elementos do rdd
numeros.top(5)

#Ver todos os elementos do rdd
numeros.collect()

#Operacoes aritmeticas
numeros.count()
numeros.sum()
numeros.mean()
numeros.stdev()
numeros.max()
numeros.min()

#Transformacoes, para ver o resultado, é necessario chamar uma acao
#filtrar numeros maior que 2 
filtro = numeros.filter(lambda filtro: filtro > 2)

#Acao
filtro.collect()

#Amostra aleatoria com ou sem reposicao
amostra = numeros.sample(True, 0.5, 1)
amostra.collect()

#Mapear os elementos do rdd
mapa = numeros.map(lambda mapa: mapa * 2)
mapa.collect()

#Uniao de rdds
numeros2 = sc.parallelize([6,7,8,9,10])
uniao = numeros.union(numeros2)
uniao.collect()

#Interseccao de rdds
interseccao = numeros.intersection(numeros2)
interseccao.collect()

#Subtracao de rdds
subtracao = numeros.subtract(numeros2)
subtracao.collect()

#Cartesiano de rdds
cartesiano = numeros.cartesian(numeros2)
cartesiano.collect()

#Contar elementos do cartesiano, ja e uma acao
cartesiano.countByValue()


#Exemplo de chave-valor
compras = sc.parallelize([(1, 200), (2, 300), (3, 120), (4, 250), (5, 78)])

chaves = compras.keys()
chaves.collect()

valores = compras.values()
valores.collect()

#Contar elementos por chave
contagem = compras.countByKey()

#Aplique uma funcao a cada valor de cada chave
soma = compras.mapValues(lambda soma: soma + 1)

#Criando um rdd que tem debitos
debitos = sc.parallelize([(1, 20), (2, 300)])

resultado = compras.join(debitos)
resultado.collect()

#Subtracao de rdds com chave-valor
semdebito = compras.subtractByKey(debitos)
semdebito.collect()