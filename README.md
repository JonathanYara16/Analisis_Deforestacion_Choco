##PROBLEMA  
Teniendo en cuenta que el Chocó es uno de los departamentos con mayor riqueza biológica de Colombia tanto de fauna como de flora, 
pero presenta una problemática continua de deforestación esto lleva a la perdida de cobertura boscosa ya sea por incendios, minerías y cultivo 
entre los periodos 2014 al 2021. 
Se busca realizar un análisis implementando el procesamiento distribuido con Apache Spark (Python/PySpark) permitiendo identificar las causas 
que generan la deforestación ene le Departamento del Chocó en los periodos mencionados anteriormente. 

##OBJETIVOS: 
- Identificar las principales causas de deforestación.
- Determinar los años con mayor pérdida de cobertura. 
- Ubicar los municipios más afectados.  
- Lograr identificar resultados mediante DataFrames y RDD.

##FUENTE DE DATOS
Origen: Datos Abiertos Colombia (www.datos.gov.co). 
Conjunto de datos: hectáreas deforestadas por año en el Chocó. 
Columnas: ID, Tipo_Geometria, Año, Imagen, Causa, Area_Hectarias, Observacion, Latitud, Longitud, Municipio.

##TECNOLOGÍAS UTILIZADAS
-Python3
- Apache Hadoop: almacenamiento 
- Apche Spark/PySpark:  procesamiento de los datos
- Matplotlib: generación de gráficos

##IMPLEMENTACIÓN DE SPARK Y TRANSFORACIÓN 

## 1 DESCARGA Y CARGA DADATASET
se leyó el conjunto de daos desde su ubicación original https://wwww.datos.gov.co/api/v3/views/iczg-dyt3/query.csv y se reposo en Hadoop en la carpeta Problema_tarea3 y dentro de está  con nombre del archivo query.csv permitiendo acceder a toda la información de forma distribuida.
Se crea archivo analisis_deforestacion.py donde reposa código para hacer carga, limpieza, transformación, exploración y visualización de los rseultados

## LIMPIEZA Y TRANSFORMACIÓN 

- Verificamos nombre de las variables
- Seleccionamos variables que solo vamos a utilizar ("id", "tipo_geometria", "a_o", "causa", "area_ha", "municipio")
- Eliminamos valores nulos
- Eliminamos duplicados con ID
- Eliminados filas con datos vacíos de la variable "area_ha"
- Completar datos faltantes en las variables "causa": "No especifica", "municipios": "No especificado", ""A_0": 0 
- Estandarizamos nombre de columnas (Primera letra Mayúscula, junto y sin espacios)
  "tipo_geometria", "Tipo_Geometria"; a_o", "Ano"; "area_ha", "Area_Hectareas" ; "municipio", "Municipio" ; "causa", "Causa"
- Estandarización de nombre de la variable Causa
  "Incendio"; "Incendio" , "Minería"| "mineria"): "Mineria" , "Cultivo": "Cultivo"

## ANÁLISIS EXPLORATORIO
- Análisis de deforestación total por causa
- Análisis de deforestación total por año
- Análisis de deforestación total por municipios

## ANÁLISIS CON RDD
- Análisis de deforestación total por causa

## GRÁFICOS 
- Por causa y total de hectáreas
- Por año y total de hectáreas
- Por municipio y total de hectáreas

## VISUALIZACIÓN DE RESULTADOS NOS PERMITIO IDENTIFICAR ENTRE EL PERIODO 20140 AL 2021 
- Nos permitió identificar como la deforestación en el choco su principal causa son los incendios con una afectación de más de 25 mil Hectáreas segudido de la minería con casi 20 mil hectáreas seguido de los cultivos con más de 12 mil hectáreas y otras aproximadamente más de 5mil hectáreas.
- Nos permitió identificar los 10 municipios con mayores afectaciones por incendios forestales en el choco ubicándose RIOSUCIO con casi 25 ml hectáreas seguida de UNGUÍA aproximadamente 13 mil hectáreas, le sigue EL ENCANTO DE SANPABLO, NÓVITA, RÍO QUITO, MEDIO ATRATO, ISTMINA, CARMEN DEL DARIEN, UNIÓN PANAMERICA, ATRATO
- Nos permitió  identificar la evolución de la forestación en el choco por año donde el 2016 hubo mayor afectaciones de está problemática con mas de 25 mil hectáreas, seguido del año 2014 con casi 15 mil hectáreas, 2019 con mas de 9 mil hectáreas despues 2021, 2020, 2015, 2017, 2018
