# Dockerfile para MySQL
FROM mysql:8.0

# Variables de entorno para configurar MySQL
ENV MYSQL_ROOT_PASSWORD=bddocker
ENV MYSQL_DATABASE=springboot_db
ENV MYSQL_USER=ADMIN
ENV MYSQL_PASSWORD=bddocker

# Configurar el timezone
ENV TZ=America/Santiago

# Exponer el puerto de MySQL
EXPOSE 3306

# Crear directorio para scripts de inicialización en caso de tenerlos
COPY ./init-scripts/ /docker-entrypoint-initdb.d/

# Configuración adicional de MySQL para compatibilidad con Spring Boot
RUN echo '[mysqld]' > /etc/mysql/conf.d/mysql-springboot.cnf && \
    echo 'default-authentication-plugin=mysql_native_password' >> /etc/mysql/conf.d/mysql-springboot.cnf && \
    echo 'character-set-server=utf8mb4' >> /etc/mysql/conf.d/mysql-springboot.cnf && \
    echo 'collation-server=utf8mb4_unicode_ci' >> /etc/mysql/conf.d/mysql-springboot.cnf

# Comando por defecto
CMD ["mysqld"]