Bringing up a local instance of DBSP
===================================

First, build a DBSP Docker image:

```
./docker.sh
```

Next, bring up an instance of the container and forward the container's port
8080 to a port on the host of your choice (e.g. 8081):

```
docker run --name dbsp -p 8081:8080 -itd dbspmanager
```

Open your browser and you should now be able to see the pipeline manager dashboard on localhost:8081.
