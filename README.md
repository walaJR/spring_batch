El microservicio:
Es un microservicio creado con spring batch cuya función es leer, procesar y escribir archivos CSV mediante chunks y multi-hilos.
Esto nos permite realizar múltiples steps de un job al mismo tiempo. Esto se traduce en mayor eficacia, eficiencia, rendimiento y mayor velocidad en el procesamiento de los registros.

Como input tenemos 3 archivos CSV base. La ejecución es la siguiente:
- Chunks establecidos en 50. Este número es recomendado para para registros >= 500 y <= 1000
- El ItemReader se encarga de leer los archivos CSV
- El ItemProcessor procesa los diversos registros de los archivos CSV. Aquí hemos puesto diversas validaciones para asegurar el buen formato de los registros del CSV. Hemos establecido políticas de validaciones para registros con valores vacíos, saldos en negativo y diversos errores en el formato
- El ItemWriter se encarga de escribir los registros del CSV en una Base de Datos MySQL
- Los registros cuyos valores de columnas sean inválidos, no serán escritos en la BD; por el contrario, se omiten y se derivan a un nuevo archivo CSV creado por el microservicio el cual tiene la nomenclatura "XXXX_errores.csv" dentro de una carpeta llamada "error-files"



Como dato extra, las versiones de las tecnologías ocupadas son:
- Java y Maven
- Springboot 3.5.4
- Spring Batch 5.2
- Base de Datos MySQL contenida en Docker
- JPA e Hibernate para la persistencia de datos
