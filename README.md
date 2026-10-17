## PROBLEMA  
Teniendo en cuenta que el Chocó es uno de los departamentos con mayor riqueza biológica de Colombia tanto de fauna como de flora, 
pero presenta una problemática continua de deforestación esto lleva a la perdida de cobertura boscosa ya sea por incendios, minerías y cultivo 
entre los periodos 2014 al 2021. 
Se busca realizar un análisis implementando el procesamiento distribuido con Apache Spark (Python/PySpark) permitiendo identificar las causas 
que generan la deforestación ene le Departamento del Chocó en los periodos mencionados anteriormente. 

## OBJETIVOS: 
- Identificar las principales causas de deforestación.
- Determinar los años con mayor pérdida de cobertura. 
- Ubicar los municipios más afectados.  
- Lograr identificar resultados mediante DataFrames y RDD.

## FUENTE DE DATOS
Origen: Datos Abiertos Colombia (www.datos.gov.co). 
Conjunto de datos: hectáreas deforestadas por año en el Chocó. 
Columnas: id, tipo_geometria, a_o, imagen, causa, area_ha, observacion, latitud, longitud, municipio.
Total registros: 7,937

## TECNOLOGÍAS UTILIZADAS
Python3
Apache Hadoop: almacenamiento 
Apche Spark/PySpark:  procesamiento de los datos
Matplotlib: generación de gráficos

## IMPLEMENTACIÓN DE SPARK Y TRANSFORACIÓN 

## DESCARGA Y CARGA DADATASET
se leyó el conjunto de daos desde su ubicación original https://wwww.datos.gov.co/api/v3/views/iczg-dyt3/query.csv y se reposo en Hadoop en la carpeta Problema_tarea3 y dentro de está  con nombre del archivo query.csv permitiendo acceder a toda la información de forma distribuida.
Se crea archivo analisis_deforestacion.py donde reposa código para hacer carga, limpieza, transformación, exploración y visualización de los rseultados


## LIMPIEZA Y TRANSFORMACIÓN 
 -Verificamos nombre de las variables
```python
VARIABLES EXISTENTES EN EL DATASET:
':id'
':version'
':created_at'
':updated_at'
'id'
'tipo_geometria'
'a_o'
'imagen'
'causa'
'area_ha'
'observacion'
'latitud'
'longitud'
'municipio'


root
 |-- id: integer (nullable = true)
 |-- tipo_geometria: string (nullable = true)
 |-- a_o: integer (nullable = true)
 |-- causa: string (nullable = true)
 |-- area_ha: double (nullable = true)
 |-- municipio: string (nullable = true)

 

+----+--------------+----+-------+--------+---------+
|  id|tipo_geometria| a_o|  causa| area_ha|municipio|
+----+--------------+----+-------+--------+---------+
|7463|       Polygon|2020|Cultivo|0.126512|   ACANDÍ|
|4659|       Polygon|2019|Cultivo| 0.14072|   ACANDÍ|
|7459|       Polygon|2020|Cultivo| 0.14125|   ACANDÍ|
|9719|       Polygon|2021|Cultivo|0.158066|   ACANDÍ|
|7464|       Polygon|2020|Cultivo|0.184549|   ACANDÍ|
+----+--------------+----+-------+--------+---------+
only showing top 5 rows
```
- Seleccionamos variables que solo vamos a utilizar ("id", "tipo_geometria", "a_o", "causa", "area_ha", "municipio")
- Eliminamos valores nulos
- Eliminamos duplicados con ID
- Eliminados filas con datos vacíos de la variable "area_ha"
- Completar datos faltantes en las variables "causa": "No especifica", "municipios": "No especificado", ""A_0": 0 
- Estandarizamos nombre de columnas (Primera letra Mayúscula, junto y sin espacios)
  "tipo_geometria", "Tipo_Geometria"; a_o", "Ano"; "area_ha", "Area_Hectareas" ; "municipio", "Municipio" ; "causa", "Causa"
- Estandarización de nombre de la variable Causa
  "Incendio"; "Incendio" , "Minería"| "mineria"): "Mineria" , "Cultivo": "Cultivo"
```python
+---+--------------+---+-----+-------+---------+
| id|tipo_geometria|a_o|causa|area_ha|municipio|
+---+--------------+---+-----+-------+---------+
|  0|             0|  0|  219|      0|        0|
+---+--------------+---+-----+-------+---------+

Tras eliminar duplicados por ID: 7,937 registros
Eliminados registros in áreas: 7,937 restantes
Datos rellenados y listos: 7,937 registros finales
Datos limpios: 7,937 registros

```
## ANÁLISIS EXPLORATORIO
- Análisis de deforestación total por causa
```python
+--------+------------------+
|   Causa|   Total_Hectareas|
+--------+------------------+
|Incendio|      25715.042708|
| Mineria|19378.913108000004|
| Cultivo|12807.391442999973|
|    Otra| 6068.934614999997|
+--------+------------------+
```
- Análisis de deforestación total por año
 ```python 
 Deforestación por Año:
+----+------------------+
| Ano|   Total_Hectareas|
+----+------------------+
|2014|14456.811081999982|
|2015|2467.7388419999984|
|2016|26511.879030999986|
|2017|1943.7328099999993|
|2018|1396.6046719999995|
|2019| 9344.500497000015|
|2020|3397.2582140000013|
|2021| 4451.756725999999|
+----+------------------+
```
- Análisis de deforestación total por municipios
  ```python
  Top 10 Municipios más afectados:
 ```python
+--------------------+------------------+
|           Municipio|   Total_Hectareas|
+--------------------+------------------+
|            RIOSUCIO|22823.823405999963|
|              UNGUÍA|12553.555074999998|
|EL CANTON DEL SAN...| 3139.747386000002|
|              NÓVITA|2920.0302669999996|
|           RÍO QUITO|2371.9421270000007|
|        MEDIO ATRATO|2268.9071760000006|
|             ISTMINA|1779.9223029999976|
|   CARMEN DEL DARIEN|1771.3643679999998|
|  UNIÓN PANAMERICANA|1531.9702609999997|
|              ATRATO|1320.1302229999994|
+--------------------+------------------+
```

## ANÁLISIS CON RDD
- Resultados del análisis de deforestación total por causa
  Incendio ------ 25715.04 hectáreas
  Mineria ------- 19378.91 hectáreas
  Cultivo ------- 12807.39 hectáreas
  Otra ----------- 6068.93 hectáreas
  

## GRÁFICOS 
- Por causa y total de hectáreas
- Por año y total de hectáreas
- Por municipio y total de hectáreas

## VISUALIZACIÓN DE RESULTADOS NOS PERMITIO IDENTIFICAR ENTRE EL PERIODO 20140 AL 2021 
- Nos permitió identificar como la deforestación en el choco su principal causa son los incendios con una afectación de más de 25 mil Hectáreas segudido de la minería con casi 20 mil hectáreas seguido de los cultivos con más de 12 mil hectáreas y otras aproximadamente más de 5mil hectáreas.
- Nos permitió identificar los 10 municipios con mayores afectaciones por incendios forestales en el choco ubicándose RIOSUCIO con casi 25 ml hectáreas seguida de UNGUÍA aproximadamente 13 mil hectáreas, le sigue EL ENCANTO DE SANPABLO, NÓVITA, RÍO QUITO, MEDIO ATRATO, ISTMINA, CARMEN DEL DARIEN, UNIÓN PANAMERICA, ATRATO
- Nos permitió  identificar la evolución de la forestación en el choco por año donde el 2016 hubo mayor afectaciones de está problemática con mas de 25 mil hectáreas, seguido del año 2014 con casi 15 mil hectáreas, 2019 con mas de 9 mil hectáreas despues 2021, 2020, 2015, 2017, 2018
