#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)
#imprimimos el esquema
df.printSchema()
#Seleccionamos las columnas a trabajar
data_work = df.select("genero", "nivelEscolaridad","nombredepartamentoatencion","tipopoblacion","rangoedad","rangobeneficioconsolidadoasignado","cantidaddebeneficiarios"
)

# Muestra las primeras filas del DataFrame
data_work.printSchema()
data_work.show()

#Filtramos para el primer analisis -- cantidad segun tipo de poblacion vulnerables


# Filtrar valores diferentes de 'ND', eliminando espacios y excluyendo valores nulos
data_analisis1 = data_work.filter((F.trim(F.col('tipopoblacion')) != 'ND') & (F.col('tipopoblacion').isNotNull())) \
                          .select('tipopoblacion', 'cantidaddebeneficiarios')

data_analisis1.show()

# Agrupar por 'tipopoblacion' y sumar la columna 'cantidaddebeneficiarios'
resultado1 = data_analisis1.groupBy('tipopoblacion').agg(F.sum('cantidaddebeneficiarios').alias('total_beneficiarios'))
resultado1 = resultado1.sort(F.col('total_beneficiarios').desc())

print("Beneficiarios segun su tipo de poblacion\n")
# Mostrar el resultado
resultado1.show()

#Filtramos para el segundo analisis -- cantidad segun su ubicación y el rango asignado


# Filtrar valores diferentes de 'ND', eliminando espacios y excluyendo valores nulos
data_analisis2 = data_work.filter((F.trim(F.col('nombredepartamentoatencion')) != 'ND') & (F.col('nombredepartamentoatencion').isNotNull())) \
                          .select('nombredepartamentoatencion','rangobeneficioconsolidadoasignado', 'cantidaddebeneficiarios')

data_analisis2.show()

# Agrupar por 'nombredepartamentoatencion' y 'rangobeneficioconsolidadoasignado' y sumar la columna 'cantidaddebeneficiarios'
resultado2 = data_analisis2.groupBy('nombredepartamentoatencion','rangobeneficioconsolidadoasignado').agg(F.sum('cantidaddebeneficiarios').alias('total_beneficiarios'))
resultado2 = resultado2.sort(F.col('total_beneficiarios').desc())

print("Beneficiarios segun el beneficio asignado y su ubicacion\n")
# Mostrar el resultado
resultado2.show(50)

#Filtramos para el tercer analisis -- cantidad segun su rango de edad


# Filtrar valores diferentes de 'ND', eliminando espacios y excluyendo valores nulos
data_analisis3 = data_work.filter((F.trim(F.col('rangoedad')) != 'ND') & (F.col('rangoedad').isNotNull())) \
                          .select('rangoedad', 'cantidaddebeneficiarios')

data_analisis3.show()

# Agrupar por 'rangoedad' y sumar la columna 'cantidaddebeneficiarios'
resultado3 = data_analisis3.groupBy('rangoedad').agg(F.sum('cantidaddebeneficiarios').alias('total_beneficiarios'))
resultado3 = resultado3.sort(F.col('total_beneficiarios').desc())

print("Beneficiarios segun el rango de edad\n")
# Mostrar el resultado
resultado3.show(10)

