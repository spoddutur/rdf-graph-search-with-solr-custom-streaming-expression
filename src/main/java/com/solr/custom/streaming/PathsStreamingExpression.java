package com.solr.custom.streaming;

import com.solr.custom.streaming.model.Path;
import com.solr.custom.streaming.model.Paths;
import com.solr.custom.streaming.model.TupleRecord;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PathsStreamingExpression extends TupleStream implements Expressible {

  // streaming expression query params
  private String collectionName;
  private String fromNode;
  private String fromField;
  private String toNode;
  private String toField;
  private String fl;
  private int maxDepth;

  private StreamExpression expression;
  private StreamFactory factory;
  private StreamContext streamContext;

  // Holder for all the streams spawned to compute path from 'fromNode' to 'toNode'.
  // Used to close streams in close() method.
  private List<CloudSolrStream> streams = new ArrayList<CloudSolrStream>();

  // Keeps track of current depth.
  private int currentDepth = 0;

  // Map of explored nodes and its neighbours.
  private HashMap<String, HashSet<TupleRecord>> neighbours = new HashMap<String, HashSet<TupleRecord>>();

  // Final result paths calculated from 'fromNode' to 'toNode'.
  private Paths paths;
  private Iterator<Path> pathsIt;

  public PathsStreamingExpression(StreamExpression expression, StreamFactory factory) throws IOException{

    this.expression = expression;
    this.factory = factory;

    this.collectionName = factory.getValueOperand(expression, 0);
    // collection name
    if(null == collectionName) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // from:FromField->FromNode
    StreamExpressionNamedParameter fromExpression = factory.getNamedOperand(expression, "from");
    if (fromExpression == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - from param is required", expression));
    } else {
      String fromExpr = ((StreamExpressionValue) fromExpression.getParameter()).getValue();
      String[] fields = fromExpr.split("->");
      if (fields.length != 2) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - from param separated by an -> and must contain two fields", expression));
      }
      this.fromField = fields[0].trim();
      this.fromNode = fields[1].trim();
    }

    // to:toField->toNode
    StreamExpressionNamedParameter toExpression = factory.getNamedOperand(expression, "to");
    if (toExpression == null) {
      throw new IOException(String.format(Locale.ROOT, "invalid expression %s - to param is required", expression));
    } else {
      String toExpr = ((StreamExpressionValue) toExpression.getParameter()).getValue();
      String[] fields = toExpr.split("->");
      if (fields.length != 2) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - to param separated by an -> and must contain two fields", expression));
      }
      this.toField = fields[0].trim();
      this.toNode = fields[1].trim();
    }

    // fl: list of fields to return per document in the query result. Defaults to "*" i.e., all fields
    StreamExpressionNamedParameter flExpression = factory.getNamedOperand(expression, "fl");
    if (flExpression == null) {
      this.fl = "*";
    } else {
      this.fl = ((StreamExpressionValue) flExpression.getParameter()).getValue();
    }

    // maxDepth
    StreamExpressionNamedParameter maxDepthExpression = factory.getNamedOperand(expression, "maxDepth");
    if (maxDepthExpression == null) {
      this.maxDepth = 10;
    } else {
      try {
        this.maxDepth = Integer.parseInt(((StreamExpressionValue) maxDepthExpression.getParameter()).getValue());
      } catch (Exception ex) {
        throw new IOException(String.format(Locale.ROOT, "invalid expression %s - maxDepth param must be a valid integer", expression));
      }

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

  @Override
  public void open() throws IOException {

    // Given from field and to field, from and to values, recursively search() till maxDepth iterations and build neighbourNodes.
    // NeighbourNodes information is then used to construct paths from 'fromNode' to 'toNode'.
    HashSet<String> parentNodes = new HashSet<String>();
    parentNodes.add(this.fromNode);
    while(this.currentDepth < this.maxDepth && !parentNodes.isEmpty()) {
      CloudSolrStream stream = constructStream(parentNodes);
      HashSet<Tuple> tuples = openAndReadStream(stream);
      parentNodes.clear();
      parentNodes.remove(this.toNode);

      // parse edges and construct neighbours
      for (Tuple tuple : tuples) {
        String src = tuple.getString(this.fromField);
        String dst = tuple.getString(this.toField);

        parentNodes.add(dst);
        if (this.neighbours.containsKey(src)) {
          this.neighbours.get(src).add(new TupleRecord(tuple));
        } else {
          HashSet<TupleRecord> dstSet = new HashSet<TupleRecord>();
          dstSet.add(new TupleRecord(tuple));
          this.neighbours.put(src, dstSet);
        }
      }
    }

    //////////// Got neighbours. Now construct graph paths from 'fromNode' to 'toNode' ////////////

    HashMap<String, Paths> endNodeToItsLinks = new HashMap<String, Paths>();
    parentNodes.clear();
    String src = this.fromNode;
    String dst = this.toNode;
    parentNodes.add(src);

    while(!parentNodes.isEmpty() && this.maxDepth-- > -1) {
      HashSet<String> newParents = new HashSet<String>();
      for (String parent: parentNodes) {
        // foreach child, add endNodeLink
        if(!this.neighbours.containsKey(parent)) {
          continue;
        }

        for (Tuple childTuple: this.neighbours.get(parent)) {

          String child = childTuple.getString(this.toField);
          newParents.add(childTuple.getString(this.toField));
          if(endNodeToItsLinks.containsKey(child)) {
            Paths parentPaths = endNodeToItsLinks.get(parent);
            Paths childPaths = endNodeToItsLinks.get(child);
            for (Path path: parentPaths.getPaths()) {
              childPaths.addPath(path.clone().add(childTuple));
            }
          } else {
            if(endNodeToItsLinks.containsKey(parent)) {
              Paths parentPaths = endNodeToItsLinks.get(parent);
              Paths childPaths = new Paths();
              for (Path path: parentPaths.getPaths()) {
                childPaths.addPath(path.clone().add(childTuple));
              }
              endNodeToItsLinks.put(childTuple.getString(this.toField), childPaths);
            }
            else {
              // adding only child now. Earlier it was (parent, child)
              Path p = Path.getInstance(Stream.of(childTuple)
                      .collect(Collectors.toCollection(ArrayList<Tuple>::new)));
              endNodeToItsLinks.put(childTuple.getString(this.toField), Paths.getInstance(p));
            }
          }
        }
        if(!dst.equals(parent)) {
          endNodeToItsLinks.remove(parent);
        }
      }
      parentNodes.clear();
      // newParents.removeAll(allVisited);
      newParents.remove(dst);
      parentNodes.addAll(newParents);
    }

    this.paths = endNodeToItsLinks.get(dst);
    this.pathsIt = paths.getPaths().iterator();
  }

  private HashSet<Tuple> openAndReadStream(CloudSolrStream stream) {
    HashSet<Tuple> edges = new HashSet<Tuple>();
    try {
      stream.open();
      BATCH:
      while (true) {
        Tuple tuple = stream.read();
        if (tuple.EOF) {
          this.currentDepth++;
          break BATCH;
        }
        edges.add(tuple);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return edges;
  }

  private CloudSolrStream constructStream(HashSet<String> srcNodes) throws IOException {

    // construct OR query
    StringBuffer sb = new StringBuffer();
    for(String srcNode: srcNodes) {
      sb.append(this.fromField + ":");
      sb.append(srcNode);
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
    if(this.pathsIt.hasNext()) {
      Path p = this.pathsIt.next();
      Tuple t = new Tuple();
      t.put("traversal", p.toString());
      return t;
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