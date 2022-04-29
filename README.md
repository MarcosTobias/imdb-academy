## Technologies needed for the application
* Docker

## Installation and setup
Download the code:
```
git clone https://github.com/MarcosTobias/imdb-academy.git
```
&nbsp;

Then, set up the elastic container:
```
cd imdb-academy

docker-compose up --build
```
It takes some time, have patience.

After running the command the terminal hangs, so you might need to open a new one
&nbsp;

For looking up documentation about the endpoints, 
you may check the Swagger documentation once the app is
running at:
```
http://localhost:8080/swagger-ui/index.html
```
There is a dropdown in the top right corner for selecting documentation regarding indexing or for searching
&nbsp;

Indexing operations are protected. For using them, the request
must contain a header with:
```
key: x-api-key                  

value: searchRules
```
##Important
If you want to do indexing operations, the tsv file must be on the following path:
```
/Users/$USER/Documents/IMDBs
```
&nbsp;


If you want to make requests directly to the elastic container, you need to add basic auth to the request.

The container is allocated on port 9200.
The auth should be as follows:
```
username: elastic  
password: searchPathRules
```