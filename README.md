# rdf-graph-search-with-solr-custom-streaming-expression
A custom query `paths()` was implemented as streaming expression in this project to find connections between any two entities in the graph. (Assuming that RDF graph is indexed in solr). `paths(from, to)` query will return all the paths connecting 'from' and 'to' as per solr index

## 1. Demo Time
1. Let's index some sample data about billgates and microsoft 
2. Once the index is ready, search using paths() query to find all the paths connecting these two entities in this graph.

Let me walk you step-by-step starting from index to query and see the results

### 1.1 Sample Data
<img width="500" src="https://user-images.githubusercontent.com/22542670/32425782-9b96cc6a-c2db-11e7-986b-1cea68ca6548.png"/>

### 1.2 Sample Query
Find all connections between BillGates and Microsoft

```markdown
      paths(rdf, 
      from="src_s->billgates",
      to="dst_s->microsoft",
      fl="src_s,dst_s,relation_s")
```
![image](https://user-images.githubusercontent.com/22542670/32426785-c62bc4ba-c2e2-11e7-9379-8055932d67a3.png)

### 1.3 Sample Query Results
Found 4 different paths connecting billgates and microsoft

<img width="574" src="https://user-images.githubusercontent.com/22542670/32427498-bbf3f374-c2e6-11e7-85be-d83f75679c6e.png"/>

## 2. Architecture and Features:
- Merges search with parallel computing (paralelly computes query in all shards and merges the results).
- Fully Streaming (no big buffers).
- SolrCloud aware.

![image](https://user-images.githubusercontent.com/22542670/32426660-f329f0a0-c2e1-11e7-8bb1-625b12407078.png)

### 2.1 Query syntax:
```markdown
paths(<collectionName>, 
      from="fromField->fromNode",
      to="toField->toNode",
      fl="<csv of fields to return per matching document>",
      maxDepth="<maximum depth to go searching for toNode starting from fromNode>")
```
## 3. Compile and Run  
### 3.1 Generate jar file
`mvn clean package` generates jar file.

### 3.2 Create .system collection
```markdown
curl 'http://localhost:8983/solr/admin/collections?action=CREATE&name=.system'
curl http://localhost:8983/solr/.system/config -d '{"set-user-property": {"update.autoCreateFields":"false"}}'
```
### 3.3 Upload jar file to .system collection
```markdown
curl -X POST -H 'Content-Type: application/octet-stream' --data-binary @rdf-graph-search-with-solr-custom-streaming-expression-1.0-SNAPSHOT.jar 'http://localhost:8983/solr/.system/blob/test'
```

### 3.4 Verify that this jar is uploaded under the name 'test' in .system collection
```markdown
curl 'http://localhost:8983/solr/.system/blob?omitHeader=true'
```

### 3.5 Add our jar as runtime-lib to test collection
```markdown
curl 'http://localhost:8983/solr/test/config' -H 'Content-type:application json' -d '{   "add-runtimelib": { "name":"test", "version":1 }}'
```
### 3.6 Register our paths() custom streaming expression with test collection
```markdown
curl 'http://localhost:8983/solr/rdf/config' -H 'Content-type:application/json' -d '{
  "create-expressible": {
    "name": "paths",
    "class": "com.solr.custom.streaming.PathsStreamingExpression",
    "runtimeLib": true
  }
}'
```

Tht's it!! Now, navigate to "stream" tab of test collection in SolrAdmin page and run your query..

## 4. How to update solr about any changes done to the jar?
### 4.1 Upload new jar with changes again
```markdown
curl -X POST -H 'Content-Type: application/octet-stream' --data-binary @custom-streaming-expression-1.0-SNAPSHOT.jar 'http://localhost:8983/solr/.system/blob/test'
```
### 4.2 Check the new version number.
Now you should see that the jar version number has increased by 1. Let's say the version is now increased from 1 to 2.
```markdown
curl 'http://localhost:8983/solr/.system/blob?omitHeader=true'
```
### 4.3 Update 'test' collection to use version 2 of our jar
```markdown
curl 'http://localhost:8983/solr/test/config' -H 'Content-type:application json' -d '{   "update-runtimelib": { "name":"test", "version":2 }}'
```
No need to restart solr. Your changes are reflected and you can start querying solr to test your results.

### 4.4. How to remove the custom expression from a collection
```markdown
curl 'http://localhost:8983/solr/test/config' -H 'Content-type:application/json' -d '{
  "delete-expressible": "mysearch"
}'
```
