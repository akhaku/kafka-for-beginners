Run the container with docker
```
docker run -it -p 9092:9092 -p 2181:2181 --hostname kafka --name kafka --rm akhaku/kafka-with-utils
```

Make sure /etc/hosts has the following
```
127.0.0.1 kafka
```
Now you can use localhost as your broker servers config.
