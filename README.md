## Technologies needed for the application
* Java 17
* Docker
* Springboot 2.6.6

## Installation and setup
Download the code:
```
git clone https://github.com/MarcosTobias/imdb-academy.git
```
&nbsp;

Then, set up the elastic container:
```
cd imdb-academy

docker-compose up
```
&nbsp;

build the application with maven:
```
mvn compile
```
&nbsp;

And then run the app:
```
mvn spring-boot:run
```
&nbsp;

For looking up documentation about the endpoints, 
you may check the Swagger documentation once the app is
running at:
```
http://localhost:8080/swagger-ui/index.html
```
&nbsp;

Indexing operations are protected. For using them, the request
must contain a header with:
```
key: x-api-key                  

value: searchRules
```