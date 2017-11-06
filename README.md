# rdf-graph-search-with-solr-custom-streaming-expression
Given a RDF graph indexed in Solr, a custom streaming expression was implemented in this project to find connections between any two entities in the graph

- Query RDF linked data for links between two entities.
- Implemented a custom stream query expression named `paths()` for SolrCloud that merges search with parallel computing.
- Fully Streaming (no big buffers).
- SolrCloud aware.

![image](https://user-images.githubusercontent.com/22542670/32425782-9b96cc6a-c2db-11e7-986b-1cea68ca6548.png)
![image](https://user-images.githubusercontent.com/22542670/32426077-af99e984-c2dd-11e7-953f-b85d5e368061.png)
![image](https://user-images.githubusercontent.com/22542670/32426110-f591ac7e-c2dd-11e7-886f-f2a2d4b2ceee.png)

## Sample Query: 
Find all connections between BillGates and Microsoft

```markdown
      paths(rdf, 
      from="src_s->billgates",
      to="dst_s->microsoft",
      fl="src_s,dst_s,relation_s")```

## Sample Query Result:
{
  "result-set": {
    "docs": [
      {
        "traversal": "[{src_s=billgates, relation_s=married, dst_s=melindagates}, {src_s=melindagates, relation_s=director, dst_s=microsoft}]"
      },
      {
        "traversal": "[{src_s=billgates, relation_s=located, dst_s=united_states}, {src_s=united_states, relation_s=organization, dst_s=microsoft}]"
      },
      {
        "traversal": "[{src_s=billgates, relation_s=developed, dst_s=windows}, {src_s=windows, relation_s=product, dst_s=microsoft}]"
      },
      {
        "traversal": "[{src_s=billgates, relation_s=co-founder, dst_s=microsoft}]"
      },
      {
        "EOF": true,
        "RESPONSE_TIME": 67
      }
    ]
  }
}
      
