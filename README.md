# rdf-graph-search-with-solr-custom-streaming-expression
Given RDF graph indexed in Solr, a custom streaming expression was written to find connection between any two entities in RDF graph

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
      
