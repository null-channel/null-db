# Log based Key/Value store 

A go implementation of a log based key value store from the null channel's building a database series 

=> link: https://www.youtube.com/playlist?list=PL5JFPVMx5WzV_7j2RoTc7hkx0os4wDJTx


Pros: 
    - it works?
    - fast writes 

Cons:
   - reads are slow as the log file grows 


## Todo's

 - [] implement log compaction
 - [] implement partitioning 



## Usage 

Start the server using 

```bash
make run
``` 


### adding a key 

```shell
curl -d '{"key":"hey","value":"world"}' -H "Content-Type: application/json" http://localhost:4000/set
```

### retreiving a value 

```shell
curl http://localhost:4000/get/key  
```

### Deleting a value 
```shell
curl -d '{"key":"hey"}' -H "Content-Type: application/json" http://localhost:4000/pop 
```

## Clean up 

`make clean`
 
