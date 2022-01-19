# In memory datastore 

A basic key value store. 

Cons:
 -  does not support replication 
 -  no backup support 
 
Pros: 
 - Fast
 - It works!?
 
Future plans 

- [] support replication 
- [] support more data types 


## How to run 

to get started run `make run`

### adding a key 

```shell
curl -d '{"key":"hey","value":"world"}' -H "Content-Type: application/json" http://localhost:4000/set
```

### retreiving a value 

```shell
curl -d '{"key":"hey"}' -H "Content-Type: application/json" http://localhost:4000/get  
```

### Deleting a value 
```shell
curl -d '{"key":"hey"}' -H "Content-Type: application/json" http://localhost:4000/pop 
```

## Clean up 

`make clean`
