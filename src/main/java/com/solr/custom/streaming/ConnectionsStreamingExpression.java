package com.solr.custom.streaming;

import com.solr.custom.streaming.model.Path;
import com.solr.custom.streaming.model.Paths;
import com.solr.custom.streaming.model.TupleRecord;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;
import org.apache.solr.common.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionsStreamingExpression extends TupleStream implements Expressible {

  // streaming expression query params
  private String collectionName;
  private String fromField = "src_s";
  private String toField = "dst_s";
  private String relationField = "relation_s";
  private HashMap<String, String> containingNodesNameToTypeMap = new HashMap<String, String>();
  private ArrayList<String> persons = new ArrayList<String>();
  private ArrayList<String> companies = new ArrayList<String>();
  private HashMap<String, String> personNameToId = new HashMap<String, String>();
  private HashMap<String, String> companyNameToId = new HashMap<String, String>();
  private String fl;

  private StreamExpression expression;
  private StreamFactory factory;
  private StreamContext streamContext;

  // Holder for all the streams spawned to compute connections.
  // Used to close streams in close() method.
  private List<CloudSolrStream> streams = new ArrayList<CloudSolrStream>();

  // Keeps track of current depth.
  private int currentDepth = 0;

  // Map of explored nodes and its neighbours.
  private HashMap<String, HashSet<TupleRecord>> neighbours = new HashMap<String, HashSet<TupleRecord>>();

  // Final result paths calculated from 'fromNode' to 'toNode'.
  private ArrayList<Tuple> output = new ArrayList<Tuple>();
  private Iterator<Tuple> outputIt;
  private ArrayList<Tuple> idTuples = new ArrayList<Tuple>();

  public ConnectionsStreamingExpression(StreamExpression expression, StreamFactory factory) throws IOException{

    this.expression = expression;
    this.factory = factory;

    this.collectionName = factory.getValueOperand(expression, 0);
    // collection name
    if(null == collectionName) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // nodes:company->microsoft && location->united states && person->bill gates
    StreamExpressionNamedParameter nodesExpression = factory.getNamedOperand(expression, "nodes");
    if (nodesExpression == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - nodes param is required", expression));
    } else {
      String nodesExpr = ((StreamExpressionValue) nodesExpression.getParameter()).getValue();
      String[] nodes = nodesExpr.split("&&");
      if (nodes.length < 2) {
        throw new IOException(String.format(Locale.ROOT, "invalid nodes param %s - it must contain atleast two nodes", expression));
      }
      int hasCompany = 0;
      int hasPerson = 0;
      for(String node: nodes) {
        String[] nodeTypeAndName = node.trim().split("->");
        String nodeType = nodeTypeAndName[0].trim();
        String nodeName = nodeTypeAndName[1].trim();
        if("company".equalsIgnoreCase(nodeType)) {
          hasCompany++; companies.add(nodeName);
        } else if("person".equalsIgnoreCase(nodeType)) {
          hasPerson++; persons.add(nodeName);
        }
        else {
          this.containingNodesNameToTypeMap.put(nodeName, nodeType);
        }
      }
//      if(hasCompany+hasPerson <2) {
//        throw new IOException(String.format(Locale.ROOT, "invalid nodes param %s - companies and persons combined count should be atleast 2", expression));
//      }
    }

    // fl: list of fields to return per document in the query result. Defaults to "*" i.e., all fields
    StreamExpressionNamedParameter flExpression = factory.getNamedOperand(expression, "fl");
    if (flExpression == null) {
      this.fl = "*";
    } else {
      this.fl = ((StreamExpressionValue) flExpression.getParameter()).getValue();
    }
  }

  @Override
  public Map toMap(final Map<String, Object> map) {
    return super.toMap(map);
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  @Override
  public List<TupleStream> children() {
    return null;
  }

  public static void main(String[] args) throws Exception {
    StreamFactory sf = new StreamFactory().withCollectionZkHost("test", "172.20.2.169:9983");// "localhost:9983");
    StreamExpression expr = new StreamExpression("connections")
            .withParameter((StreamExpressionParameter) (new StreamExpressionValue("test")))
            .withParameter(new StreamExpressionNamedParameter("nodes", "company->microsoft && location->united states && person->billgates"))
            .withParameter(new StreamExpressionNamedParameter("fl", "src_s,relation_s,dst_s"));

    StreamContext sc = new StreamContext();
    sc.setStreamFactory(sf);
    SolrClientCache scc = new SolrClientCache();
    scc.getCloudSolrClient("172.20.2.169:9983");
    sc.setSolrClientCache(scc);

    ConnectionsStreamingExpression pse = new ConnectionsStreamingExpression(expr, sf);
    pse.setStreamContext(sc);
    pse.open();
  }
  @Override
  public void open() throws IOException {

    getIds(companies);
    getIds(persons);

    // Given from field and to field, from and to values, recursively search() till maxDepth iterations and build neighbourNodes.
    // NeighbourNodes information is then used to construct paths from 'fromNode' to 'toNode'.
    HashSet<String> parentNodes = new HashSet<String>();
    parentNodes.addAll(companyNameToId.values());
    parentNodes.addAll(personNameToId.values());

    CloudSolrStream stream = constructStream(parentNodes);
    openAndReadStream(parentNodes, stream);

    //////////// Got neighbours. Now construct graph paths from 'fromNode' to 'toNode' ////////////

    HashSet<String> nodesConnections = new HashSet<String>();
    nodesConnections.addAll(containingNodesNameToTypeMap.keySet());
    nodesConnections.addAll(companyNameToId.values());
    nodesConnections.addAll(personNameToId.values());

    String[] nodesConnectionsArray = nodesConnections.toArray(new String[0]);
    for(int i=0; i<nodesConnections.size(); i++) {
      String ithNode = nodesConnectionsArray[i];

      for(int j=i+1; j<nodesConnections.size(); j++) {
        String jthNode = nodesConnectionsArray[j];
        String keyToLookup1 = ithNode+"->"+jthNode;
        String keyToLookup2 = jthNode+"->"+ithNode;

        if(this.neighbours.containsKey(keyToLookup1)) {
          this.output.addAll(this.neighbours.get(keyToLookup1));
        } else if(this.neighbours.containsKey(keyToLookup2)) {
          this.output.addAll(this.neighbours.get(keyToLookup2));
        }
      }
    }

    if(this.output.size() > 0) {
      this.output.addAll(this.idTuples);
    }

    this.outputIt = this.output.iterator();
  }

  private void openAndReadStream(HashSet<String> srcNodes, CloudSolrStream stream) {

    try {
      stream.open();
      BATCH:
      while (true) {
        Tuple tuple = stream.read();
        if (tuple.EOF) {
          this.currentDepth++;
          break BATCH;
        }

        String fromNode = tuple.getString(this.fromField);
        String toNode = tuple.getString(this.toField);
        String srcNode = (srcNodes.contains(fromNode)) ? fromNode : toNode;
        String dstNode = (srcNodes.contains(fromNode)) ? toNode : fromNode;
        String key = srcNode+"->"+dstNode;
        if (this.neighbours.containsKey(key)) {
          this.neighbours.get(key).add(new TupleRecord(tuple));
        } else {
          HashSet<TupleRecord> dstSet = new HashSet<TupleRecord>();
          dstSet.add(new TupleRecord(tuple));
          this.neighbours.put(key, dstSet);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private CloudSolrStream constructStream(HashSet<String> srcNodes) throws IOException {

    // construct OR query
    StringBuffer sb = new StringBuffer();
    for(String srcNode: srcNodes) {
      sb.append(this.fromField + ":");
      sb.append('"'+srcNode+'"');
      sb.append(" OR ");
    }
    // int start = sb.lastIndexOf(" OR ");
    // sb.replace(start, start + 4, "");

    for(String srcNode: srcNodes) {
      sb.append(this.toField + ":");
      sb.append('"'+srcNode+'"');
      sb.append(" OR ");
    }
    int start = sb.lastIndexOf(" OR ");
    sb.replace(start, start + 4, "");

    String queryStr = sb.toString().trim();

    StreamExpression expr = new StreamExpression("search")
            .withParameter((StreamExpressionParameter) (new StreamExpressionValue(this.collectionName)))
            .withParameter(new StreamExpressionNamedParameter("q", queryStr))
            .withParameter(new StreamExpressionNamedParameter("fl", this.fl))
            .withParameter(new StreamExpressionNamedParameter("sort", this.fromField + " asc"))
            .withParameter(new StreamExpressionNamedParameter("qt", "/export"));

    this.expression = expr;
    CloudSolrStream stream = new CloudSolrStream(expr, this.factory);
    stream.setStreamContext(this.streamContext);
    this.streams.add(stream);
    return stream;
  }

  private void getIds(ArrayList<String> srcNodes) throws IOException {

    if(srcNodes == null || srcNodes.size() <= 0) {
      return;
    }
    String queryTemplate = "(relation_s:\"name\" AND dst_s:\"%s\")";

    // construct OR query
    StringBuffer sb = new StringBuffer();
    for(String srcNode: srcNodes) {
      sb.append(String.format(queryTemplate, srcNode));
      sb.append(" OR ");
    }

    int start = sb.lastIndexOf(" OR ");
    sb.replace(start, start + 4, "");

    String queryStr = sb.toString().trim();

    StreamExpression expr = new StreamExpression("search")
            .withParameter((StreamExpressionParameter) (new StreamExpressionValue(this.collectionName)))
            .withParameter(new StreamExpressionNamedParameter("q", queryStr))
            .withParameter(new StreamExpressionNamedParameter("fl", this.fl))
            .withParameter(new StreamExpressionNamedParameter("sort", this.fromField + " asc"))
            .withParameter(new StreamExpressionNamedParameter("qt", "/export"));

    this.expression = expr;
    CloudSolrStream stream = new CloudSolrStream(expr, this.factory);
    stream.setStreamContext(this.streamContext);
    this.streams.add(stream);

    try {
      stream.open();
      BATCH:
      while (true) {
        Tuple tuple = stream.read();
        if (tuple.EOF) {
          break BATCH;
        }

        this.idTuples.add(tuple);
        String id = tuple.getString(this.fromField);
        String name = tuple.getString(this.toField);

        if(this.persons.contains(name)) {
          this.personNameToId.put(name, id);
        } else {
          this.companyNameToId.put(name, id);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if(this.streams != null && !this.streams.isEmpty()) {
      for (CloudSolrStream stream : this.streams) {
        stream.close();
      }
    }
  }

  @Override
  public Tuple read() throws IOException {
    if(this.outputIt.hasNext()) {
      return this.outputIt.next();
    } else {
      return new Tuple(Collections.singletonMap("EOF", true));
    }
  }

  @Override
  public StreamComparator getStreamSort() {
    return null;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return this.expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new StreamExplanation(getStreamNodeId().toString())
        .withFunctionName("paths")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
        .withExpression("--non-expressible--");
  }
}