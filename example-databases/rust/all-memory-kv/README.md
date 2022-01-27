# All in memory

Your very basic all in memory database.

Pros:
1) FAST
1) Easy to manage

Cons:
1) Memory is expensive
1) 100% transiant
1) Key-Value store only. meaning it only supports things with a one to one relationship


These types of applications work really well as caching layers, and like redis and can grow byond this.

### THIS IS NOT A PRODUCTION DB

### So what is it missing from somethihng like Redis?
A LOT of things. security for one. But there are a lot of features you are missing. so lets talk about it.

1) it can only scale to a single node, there is no way to add another node.
1) no backup/recover
1) inability to shard
1) no tools to help with management of the database

And the list really goes on and on. A tool like redis even has the ability to do things like pub/sub and all those extra features don't exist here! :)