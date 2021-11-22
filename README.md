# Approach 01:

* create a GCP project *spring.cloud.gcp.project-id*
* ActiveMQ topic *inbound.endpoint* to publish the messages
* Read form topic convert to JSONL file of products
* Write to Cloud storage bucket *com.sck.upload.bucket* folder *com.sck.upload.backup.dir* with extension .json
* Upload to Bigquery Table *sck.gcp.bigquery.table* in dataSet *spring.cloud.gcp.bigquery.datasetName*



# Steps for Creating and running a Docker application:
## 01) Create Executable jar
    java -jar gcp-approach01.jar
    http://localhost:8080/gcp-services/swagger-ui.html
    Running with port 8090
    java -jar gcp-01-storage-bigquery.jar --server.port=8090
## 02) Create Docker File
    FROM : Base Docker Image 
  in https://hub.docker.com
    
    ADD: Executable application to the Base docker image
    
    ENTRYPOINT: Command to be executed when you run the docker container
    
## 03) Build Docker Image
* Ensure docker is up :  
  *docker -v* / *docker --version*
* List current images :  
    *docker images*
* List running containers :  
  *docker container ls*
* Build the image :  
  *docker build -t gcp-approach01 .*
  
    Build image with tag name dockersgcp-approach01 from current folder .
  
    Note: Image name should be in lower case
* List the images :  
  *docker images*

## 04) Run Docker Container
* List running processes :  
  *docker ps*
* running a new process :  
  *docker run -p8081:8090 gcp-approach01*
  
    System port 8081 is mapped to the port 8090 in docker container (Virtual machine)
    http://localhost:8081/gcp-services

# Clean up Commands


- docker ps
- docker stop <ContainerID>
- docker rm <ContainerID>
- docker rmi -f <imageName>

