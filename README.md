# rdf-graph-search-with-solr-custom-streaming-expression
Given a RDF graph indexed in Solr, a custom streaming expression was implemented in this project to find connections between any two entities in the graph

## Architecture and Features:
- Merges search with parallel computing (paralelly computes query in all shards and merges the results).
- Fully Streaming (no big buffers).
- SolrCloud aware.

![image](https://user-images.githubusercontent.com/22542670/32426660-f329f0a0-c2e1-11e7-8bb1-625b12407078.png)

## Sample Data
![image](https://user-images.githubusercontent.com/22542670/32425782-9b96cc6a-c2db-11e7-986b-1cea68ca6548.png)

## Sample Query
Find all connections between BillGates and Microsoft

```markdown
      paths(rdf, 
      from="src_s->billgates",
      to="dst_s->microsoft",
      fl="src_s,dst_s,relation_s")```
      
![image](https://user-images.githubusercontent.com/22542670/32426077-af99e984-c2dd-11e7-953f-b85d5e368061.png)


## Sample Query Results
Found 4 different paths connecting billgates and microsoft

![image](https://user-images.githubusercontent.com/22542670/32426110-f591ac7e-c2dd-11e7-886f-f2a2d4b2ceee.png)


      
