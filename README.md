El microservicio:
Es un microservicio creado con spring batch cuya función es leer, procesar y escribir archivos CSV mediante chunks y multi-hilos.

Todo el proceso de batch es mediante chunks y multi-hilos. Esto nos permite realizar múltiples steps de un job al mismo tiempo. Esto se traduce en mayor eficacia y mayor velocidad en el procesamiento de los registros.

Como input tenemos 3 archivos CSV base. La ejecución es la siguiente:
- Chunks establecidos en 50. Este número es recomendado para para registros >= 500 y <= 1000
- El ItemReader se encarga de leer los archivos csv
- El ItemProcessor procesa los diversos registros de los archivos CSV. Aquí hemos puesto diversas validaciones para asegurar el buen formato de los registros del CSV. Hemos establecido políticas de validaciones para registros con valores vacíos, saldos en negativo y diversos errores en el formato
