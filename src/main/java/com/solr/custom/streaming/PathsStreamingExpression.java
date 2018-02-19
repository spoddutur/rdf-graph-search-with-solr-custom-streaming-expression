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
  private int maxThresholdForFloodFill = 100;

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
      this.maxDepth = 3;
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

  public static void main(String[] args) throws Exception {
    StreamFactory sf = new StreamFactory().withCollectionZkHost("test", "localhost:9983");
    StreamExpression expr = new StreamExpression("paths")
            .withParameter((StreamExpressionParameter) (new StreamExpressionValue("test")))
            .withParameter(new StreamExpressionNamedParameter("from", "src_s->590"))
            .withParameter(new StreamExpressionNamedParameter("to", "dst_s->united states"))
            .withParameter(new StreamExpressionNamedParameter("fl", "src_s,relation_s,dst_s"));

    StreamContext sc = new StreamContext();
    sc.setStreamFactory(sf);
    SolrClientCache scc = new SolrClientCache();
    scc.getCloudSolrClient("localhost:9983");
    sc.setSolrClientCache(scc);

    PathsStreamingExpression pse = new PathsStreamingExpression(expr, sf);
    pse.setStreamContext(sc);
    pse.open();
  }
  @Override
  public void open() throws IOException {

    // Given from field and to field, from and to values, recursively search() till maxDepth iterations and build neighbourNodes.
    // NeighbourNodes information is then used to construct paths from 'fromNode' to 'toNode'.
    HashSet<String> parentNodes = new HashSet<String>();
    HashSet<String> allVisited = new HashSet<String>();
    parentNodes.add(this.fromNode);
    parentNodes.add(this.toNode);
    while(this.currentDepth < this.maxDepth && !parentNodes.isEmpty()) {
      CloudSolrStream stream = constructStream(parentNodes);
      HashMap<String, HashSet<Tuple>> srcNodeToEdgesMap = openAndReadStream(parentNodes, stream);

      allVisited.addAll(parentNodes);
      parentNodes.clear();
      // parentNodes.remove(this.toNode);

      // parse edges and construct neighbours
      for(String srcNode: srcNodeToEdgesMap.keySet()) {
        HashSet<Tuple> edgesOfSrcNode = srcNodeToEdgesMap.get(srcNode);
        boolean srcHasTooManyEdges = edgesOfSrcNode.size() >= maxThresholdForFloodFill;
        for (Tuple tuple : edgesOfSrcNode) {
          String from = tuple.getString(this.fromField);
          String to = tuple.getString(this.toField);

          // flood fill dst only if src is not a hot node (i.e., a node with too many links).
          if(!srcHasTooManyEdges) {
            String nextParentToVisit = srcNode.equals(from) ? to : from;
            if(!allVisited.contains(nextParentToVisit)) {
              parentNodes.add(nextParentToVisit);
            }
          }

          if (this.neighbours.containsKey(srcNode)) {
            this.neighbours.get(srcNode).add(new TupleRecord(tuple));
          } else {
            HashSet<TupleRecord> dstSet = new HashSet<TupleRecord>();
            dstSet.add(new TupleRecord(tuple));
            this.neighbours.put(srcNode, dstSet);
          }
        }
      }
    }

    //////////// Got neighbours. Now construct graph paths from 'fromNode' to 'toNode' ////////////

    System.out.println("Got neighbours" + this.neighbours);
    // Build backward paths
    HashMap<String, Paths> endNodeToItsBackwardLinks = new HashMap<String, Paths>();
    parentNodes.clear();
    allVisited.clear();
    this.currentDepth = this.maxDepth;
    String dst = this.toNode;
    String src = this.fromNode;
    parentNodes.add(dst);
    while(!parentNodes.isEmpty() && this.currentDepth-- > -1) {
      HashSet<String> newParents = new HashSet<String>();
      allVisited.addAll(parentNodes);
      for (String parent: parentNodes) {
        // foreach child, add endNodeLink
        if(!this.neighbours.containsKey(parent)) {
          continue;
        }

        for (Tuple childTuple: this.neighbours.get(parent)) {
          // System.out.println("HERE:" + endNodeToItsBackwardLinks);
          String toNode = childTuple.getString(this.toField);
          String fromNode = childTuple.getString(this.fromField);
          String srcNode = parent;
          String dstNode = parent.equals(fromNode) ? toNode : fromNode;
          String child = dstNode;
          newParents.add(dstNode);
          if(endNodeToItsBackwardLinks.containsKey(child)) {
            Paths parentPaths = endNodeToItsBackwardLinks.get(parent);
            Paths childPaths = endNodeToItsBackwardLinks.get(child);
            if(parentPaths != null && parentPaths.getPaths().size() > 0) {
              for (Path path : parentPaths.getPaths()) {
                childPaths.addPath(path.clone().add(childTuple));
              }
            }
          } else {
            if(endNodeToItsBackwardLinks.containsKey(parent)) {
              Paths parentPaths = endNodeToItsBackwardLinks.get(parent);
              Paths childPaths = new Paths();
              if(parentPaths != null && parentPaths.getPaths().size() > 0) {
                for (Path path : parentPaths.getPaths()) {
                  childPaths.addPath(path.clone().add(childTuple));
                }
              }
              endNodeToItsBackwardLinks.put(dstNode, childPaths);
            }
            else {
              // adding only child now. Earlier it was (parent, child)
              Path p = Path.getInstance(Stream.of(childTuple)
                      .collect(Collectors.toCollection(ArrayList<Tuple>::new)));
              endNodeToItsBackwardLinks.put(dstNode, Paths.getInstance(p));
            }
          }
        }
        /*if(!dst.equals(parent)) {
          endNodeToItsBackwardLinks.remove(parent);
        }*/
      }
      parentNodes.clear();
      newParents.removeAll(allVisited);
      newParents.remove(src);
      parentNodes.addAll(newParents);
    }

    // build forward paths
    HashMap<String, Paths> endNodeToItsForwardLinks = new HashMap<String, Paths>();
    parentNodes.clear();
    parentNodes.add(src);

    while(!parentNodes.isEmpty() && this.maxDepth-- > -1) {
      HashSet<String> newParents = new HashSet<String>();
      for (String parent: parentNodes) {

        boolean hasBackwardLink = endNodeToItsBackwardLinks.containsKey(parent) && endNodeToItsBackwardLinks.get(parent).getPaths() != null;
        boolean hasForwardLink = this.neighbours.containsKey(parent);

        // foreach child, add endNodeLink
        if (!hasBackwardLink && !hasForwardLink) {
          continue;
        }

        if (hasBackwardLink) {
          Paths parentPaths = endNodeToItsForwardLinks.get(parent);

          // init dst paths
          Paths dstPaths;
          if(endNodeToItsForwardLinks.containsKey(dst)) {
            dstPaths = endNodeToItsForwardLinks.get(dst);
          } else {
            dstPaths = new Paths();
          }

          for (Path backwardPathFromDstToParent : endNodeToItsBackwardLinks.get(parent).getPaths()) {
            if(parentPaths != null && parentPaths.getPaths() !=null && parentPaths.getPaths().size() >0) {
              for (Path path : parentPaths.getPaths()) {
                dstPaths.addPath(path.clone().addAll(backwardPathFromDstToParent.getNodes()));
              }
              endNodeToItsForwardLinks.put(dst, dstPaths);
            }
          }
        }

        if(hasForwardLink) {
          for (Tuple childTuple : this.neighbours.get(parent)) {

            String toNode = childTuple.getString(this.toField);
            String fromNode = childTuple.getString(this.fromField);
            String srcNode = parent;
            String dstNode = parent.equals(fromNode) ? toNode : fromNode;
            String child = dstNode;
            newParents.add(childTuple.getString(this.toField));
            if (endNodeToItsForwardLinks.containsKey(child)) {
              Paths parentPaths = endNodeToItsForwardLinks.get(parent);
              Paths childPaths = endNodeToItsForwardLinks.get(child);
              if(parentPaths == null) {
                childPaths.addPath(new Path().add(childTuple));
              } else {
                if(parentPaths != null && parentPaths.getPaths() !=null && parentPaths.getPaths().size() >0) {
                  for (Path path : parentPaths.getPaths()) {
                    childPaths.addPath(path.clone().add(childTuple));
                  }
                }
              }
            } else {
              if (endNodeToItsForwardLinks.containsKey(parent)) {
                Paths parentPaths = endNodeToItsForwardLinks.get(parent);
                Paths childPaths = new Paths();
                if(parentPaths != null && parentPaths.getPaths() !=null && parentPaths.getPaths().size() >0) {
                  for (Path path : parentPaths.getPaths()) {
                    childPaths.addPath(path.clone().add(childTuple));
                  }
                }
                endNodeToItsForwardLinks.put(childTuple.getString(this.toField), childPaths);
              } else {
                // adding only child now. Earlier it was (parent, child)
                Path p = Path.getInstance(Stream.of(childTuple)
                        .collect(Collectors.toCollection(ArrayList<Tuple>::new)));
                endNodeToItsForwardLinks.put(childTuple.getString(this.toField), Paths.getInstance(p));
              }
            }
          }
        }

        if(!dst.equals(parent)) {
          endNodeToItsForwardLinks.remove(parent);
        }
      }
      parentNodes.clear();
      // newParents.removeAll(allVisited);
      newParents.remove(dst);
      newParents.remove(src);
      parentNodes.addAll(newParents);
    }

    System.out.println("ForwardLinks:" + endNodeToItsForwardLinks);
    // System.out.println("BackwardLinks:" + endNodeToItsBackwardLinks);
    this.paths = endNodeToItsForwardLinks.get(dst);  // endNodeToItsForwardLinks.get(dst);
    System.out.println(this.paths);
    this.pathsIt = paths.getPaths().iterator();
  }

  private HashMap<String, HashSet<Tuple>> openAndReadStream(HashSet<String> srcNodes, CloudSolrStream stream) {
    HashMap<String, HashSet<Tuple>> srcNodeToEdgeMap = new HashMap<String, HashSet<Tuple>>();
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

        String fromNode = tuple.getString(this.fromField);
        String toNode = tuple.getString(this.toField);
        String srcNode = (srcNodes.contains(fromNode)) ? fromNode : toNode;
        if(!srcNodeToEdgeMap.containsKey(srcNode)) {
          HashSet<Tuple> list = new HashSet<Tuple>();
          list.add(tuple);
          srcNodeToEdgeMap.put(srcNode, list);
        } else {
          srcNodeToEdgeMap.get(srcNode).add(tuple);
        }
        // edges.add(tuple);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return srcNodeToEdgeMap;
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