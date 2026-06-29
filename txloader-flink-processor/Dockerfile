FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY target/txloader-flink-processor.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
