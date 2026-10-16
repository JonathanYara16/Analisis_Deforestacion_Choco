
#Librerias para inicio y operaciones
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, when

#Librerias para  gráficos
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#Inicio de Spark
spark = SparkSession.builder.appName("Deforestacion").getOrCreate()
print ("Inicio de sesíon")

#Cargat datos (ruta)
file_path= "hdfs://localhost:9000/Problema_tarea3/query.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True,encoding="UTF-8")
print (f"\n Total registros: {df.count():,}")

# Ver nombres exactos de las variables
print("\n" + "=" * 60)
print("VARIABLES EXISTENTES EN EL DATASET:")
print("=" * 60)
for nombre in df.columns:
    print(f"'{nombre}'")
print("=" * 60)

# Selección de variables a trabajar .
df = df.select(
    "id",
    "tipo_geometria",
    "a_o",
    "causa",
    "area_ha",
    "municipio"
)

# AQUÍ SIGUE TU CÓDIGO IGUAL — NO BORRAS NADA
df.printSchema()
df.show(5, truncate=25)

#Limpieza
print ("\n" + "="*60)
print ("Limpieza de datos")
print ("="*60)

#Datos Nulos
df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

#Eliminar duplicados ID
df = df.dropDuplicates(["ID"])
print (f"Tras eliminar duplicados por ID: {df.count():,} registros")

# Eliminar filas con datos  vacíos
campo_clave = [ "area_ha"]
df = df.na.drop(subset= campo_clave)
print (f" Eliminados registros in áreas: {df.count():,} restantes")

#Completar datos faltantes  para no eliminar otros datos que sirvan para el análisis 
df = df.fillna({
    "causa": "No especificada",
    "municipio": "No especificado",
    "A_o": 0
})

print (f" Datos rellenados y listos: {df.count():,} registros finales")

# Estandarizar nombres de columnas — Primera Mayúscula, Junto, Sin Espacios
df = df \
    .withColumnRenamed("tipo_geometria", "Tipo_Geometria") \
    .withColumnRenamed("a_o", "Ano") \
    .withColumnRenamed("area_ha", "Area_Hectareas") \
    .withColumnRenamed("municipio", "Municipio") \
    .withColumnRenamed("causa", "Causa")


# Estandarizar nombres de la variable causa
df = df.withColumn("Causa",
    when(col("Causa").contains("Incendio"), "Incendio")
    .when(col("Causa").contains("Minería")| col("Causa").contains("Mineria"),"Mineria")
    .when(col("Causa").contains("Cultivo"), "Cultivo")
    .otherwise("Otra")
)

print (f"Datos limpios: {df.count():,} registros")

# EDA CON DATAFRAMES
print ("\n" + "="*60)
print ("ANÁLISIS EXPLORATORIO")
print ("="*60)

# Deforestación total por Causa
por_causa = df.groupBy("Causa") \
    .agg(sum("Area_Hectareas").alias("Total_Hectareas")) \
    .orderBy(desc("Total_Hectareas"))
print ("\n Deforestación por Causa:")
por_causa.show()

# Deforestación por Año
por_ano = df.groupBy("Ano") \
    .agg(sum("Area_Hectareas").alias("Total_Hectareas")) \
    .orderBy("Ano")
print ("\n Deforestación por Año:")
por_ano.show()

# Deforestación por Municipios
def_municipios = df.groupBy("Municipio") \
    .agg(sum("Area_Hectareas").alias("Total_Hectareas")) \
    .orderBy(desc("Total_Hectareas")) \
    .limit(10)
print ("\n Top 10 Municipios más afectados:")
def_municipios.show()


# ANÁLISIS CON RDD
print ("\n" + "="*60)
print ("ANÁLISIS CON RDD")
print ("="*60)

rdd = df.rdd

rdd_causa = rdd \
    .map(lambda fila: (fila["Causa"], fila["Area_Hectareas"])) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: -x[1]) \
    .collect()

print ("\n Por Causa (RDD):")
for causa, area in rdd_causa:
    print (f"   {causa:15} → {area:.2f} hectáreas")

#  GRAFICOS 
pd_causa = por_causa.toPandas()
pd_ano = por_ano.toPandas()
pd_municipio = def_municipios.toPandas()

# Gráfico 1: Por Causa
plt.figure(figsize=(7, 5))
colores = {"Incendio":"#3b82f6", "Mineria":"#22c55e", "Cultivo":"#ef4444", "Otra":"#9ca3af"}
plt.bar(pd_causa["Causa"], pd_causa["Total_Hectareas"],
        color=[colores[c] for c in pd_causa["Causa"]])
plt.title("DeforestacionPorCausa — Choco", fontsize=14)
plt.ylabel("Hectareas")
plt.tight_layout()
plt.savefig("Deforestacion_causa.png", dpi=300)
print("Guardado: Deforestacion_causa.png")

# Gráfico 2: Por Año
plt.figure(figsize=(9, 5))
plt.bar(pd_ano["Ano"], pd_ano["Total_Hectareas"], color="#f97316")
plt.title("EvolucionDeforestacionPorAño — Choco", fontsize=14)
plt.xlabel("Año")
plt.ylabel("Hectareas")
plt.tight_layout()
plt.savefig("Deforestacion_anual.png", dpi=300)
print("Guardado: Deforestacion_anual.png")

# Gráfico 3: Municipios
plt.figure(figsize=(10, 6))
plt.barh(pd_municipio["Municipio"], pd_municipio["Total_Hectareas"], color="#8b5cf6")
plt.gca().invert_yaxis()
plt.title("Top10MunicipiosMasAfectados — Choco", fontsize=14)
plt.xlabel("Hectareas")
plt.tight_layout()
plt.savefig("Deforestacion_municipios.png", dpi=300)
print("Guardado: Deforestacion_municipios.png")

# Guardar Resultados
por_causa.write.csv("./resultados_por_causa", header=True, mode="overwrite")
por_ano.write.csv("./resultados_por_año", header=True, mode="overwrite")
def_municipios.write.csv("./resultados_por_municipios", header=True, mode="overwrite")

print ("\n Resultados guardados en HDFS")
spark.stop()
print (" Proceso finalizado")
def_municipios.write.csv("./resultados_por_municipios", header=True, mode="overwrite")
