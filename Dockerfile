FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN apk add --no-cache maven && mvn package -DskipTests -q

FROM eclipse-temurin:17-jre-alpine
# Librerias nativas que Alpine (musl) no trae y que el cliente de Kafka carga en tiempo de ejecucion:
#   - gcompat: el cargador de glibc (ld-linux-x86-64.so.2) que pide snappy-java. Sin el, el
#     generador de carga del perfil demo (ProductorDeCargaConfig, compression.type=snappy) falla
#     en el primer send() con UnsatisfiedLinkError.
#   - libstdc++: la pide snappy.
RUN apk add --no-cache gcompat libstdc++
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8087
ENTRYPOINT ["java", "-jar", "app.jar"]
