
# Trabajo de fin de grado de informática -  URJC - 2019/2020
### Yihui Xia

## Resumen

En este trabajo se va a profundizar en el estudio del lenguaje de programación funcional, Scala, y sobre todos de sus librerías como Spark, Plotly. Se hará queries del ámbito de desastres naturales con Spark en distintos entornos, como por ejemplo, Jupyter Notebook, Databricks, proyecto sbt y Amazon Web Service. Explicaremos el funcionamiento de cada entorno con ejemplos y también comparemos la diferencia que existe en los distintos entornos.

## Objetivos de este trabajo

1) Realizar queries en Spark SQL, tanto con Datasets como con DataFrames, y analizar las optimizaciones que ofrece Spark. 
2) Visualizar los resultados de estas queries utilizando librerías gráficas de Scala. 
3) Pruebas en la nube mediante Databricks, un servicio autogestionado de Spark. 
4) Pruebas en la nube mediante AWS, y el servicio autogestionado EMR 

Para la elaboración de este trabajo, se utilizará GitHub para el seguimiento del desarrollo y la plataforma Overleaf para su redacción.

## Contenido del repositorio

- Los códigos relacionados con el objativo 1) y 2) están en el Notebook `TFG`
- Para el objetivo 3): como los documentos de Databricks están en su plataforma, aquí está el resultado sacado con Databricks `Solucion_databricks.html`
- Para el objetivo 4): está en el proyecto sbt `workflow-example/` del cual vamos a generar el jar para ejecurar en AWS, el .jar no está subido debido a su tamaño.
- También está el notebook `Defensa de tfg` que será usado para el día de la defensa. 
- En la carpeta `data\` están las bases de datos que he utilizado salvo uno debido a su tamaño y se puede descargar de la siguiente página:
https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data?select=GlobalLandTemperaturesByCity.csv
- En la carpate `images\` están las imagenes que sale en la memoria del trabajo.


