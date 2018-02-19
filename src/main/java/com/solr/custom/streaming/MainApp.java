package com.solr.custom.streaming;

import com.solr.custom.streaming.*;
import com.solr.custom.streaming.model.Path;
import com.solr.custom.streaming.model.Paths;
import org.apache.solr.client.solrj.io.Tuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MainApp {

    /**
     * src_s,dst_s
     einstein,birth_place1
     birth_place1,ulm
     einstein,place_of_birth1
     place_of_birth1,ulm
     ulm,state1
     *state1,baden_wurttemburg
     ulm,country1
     country1,germany
     baden_wurttemburg,country1
     *state1,baden_wurttemburg
     stuffgart,capital1
     capital1,baden_wurttemburg
     stuffgart,state1
     *state1,baden_wurttemburg
     baden_wurttemburg,country2
     country2,germany
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("hi");

        List<HashMap<String, String>> l = new ArrayList<HashMap<String, String>>();
        HashMap<String, String> t1 = new HashMap<String, String>();
        t1.put("src", "a");
        t1.put("dst", "1");
        l.add(t1);
/*
        Tuple t2 = new TupleRecord();
        t2.put("src", "1");
        t2.put("dst", "2");

        Path p = new Path();
        p.add(t1);
        p.add(t2);*/

        System.out.println(l.toString());

        TreeSet<Tuple> tuples = new TreeSet<Tuple>();

        StringBuffer sb = new StringBuffer();
        for(String srcNode: Stream.of("marriage1", "located1", "cofounder1", "workedon1")
                .collect(Collectors.toCollection(HashSet::new))) {
            sb.append("src_s:");
            sb.append(srcNode);
            sb.append(" OR ");
        }
        int start = sb.lastIndexOf(" OR ");
        sb.replace(start, start + 4, "");
        System.out.println(sb.toString());

        // HashMap<String, HashSet<String>> parentToChildConn = getNeighbours();
        HashMap<String, List<Tuple>> neighbours = getBGNeighbours1();
        String src = "bg";
        String dst = "ms";
        String toField = "dst_s";
        String fromField = "src_s";

        int maxDepth = 10;
        // Build backward paths
        HashMap<String, Paths> endNodeToItsBackwardLinks = new HashMap<String, Paths>();
        HashSet<String> parentNodes = new HashSet<String>();
        parentNodes.add(src);
        parentNodes.add(dst);

        parentNodes.clear();
        parentNodes.add(dst);
        while(!parentNodes.isEmpty() && maxDepth-- > -1) {
            HashSet<String> newParents = new HashSet<String>();
            for (String parent: parentNodes) {
                // foreach child, add endNodeLink
                if(!neighbours.containsKey(parent)) {
                    continue;
                }

                for (Tuple childTuple: neighbours.get(parent)) {

                    String child = childTuple.getString(toField);
                    newParents.add(childTuple.getString(toField));
                    if(endNodeToItsBackwardLinks.containsKey(child)) {
                        Paths parentPaths = endNodeToItsBackwardLinks.get(parent);
                        Paths childPaths = endNodeToItsBackwardLinks.get(child);
                        for (Path path: parentPaths.getPaths()) {
                            childPaths.addPath(path.clone().add(childTuple));
                        }
                    } else {
                        if(endNodeToItsBackwardLinks.containsKey(parent)) {
                            Paths parentPaths = endNodeToItsBackwardLinks.get(parent);
                            Paths childPaths = new Paths();
                            for (Path path: parentPaths.getPaths()) {
                                childPaths.addPath(path.clone().add(childTuple));
                            }
                            endNodeToItsBackwardLinks.put(childTuple.getString(toField), childPaths);
                        }
                        else {
                            // adding only child now. Earlier it was (parent, child)
                            Path p = Path.getInstance(Stream.of(childTuple)
                                    .collect(Collectors.toCollection(ArrayList<Tuple>::new)));
                            endNodeToItsBackwardLinks.put(childTuple.getString(toField), Paths.getInstance(p));
                        }
                    }
                }
                /*if(!dst.equals(parent)) {
                    endNodeToItsBackwardLinks.remove(parent);
                }*/
            }
            parentNodes.clear();
            // newParents.removeAll(allVisited);
            newParents.remove(src);
            parentNodes.addAll(newParents);
        }

        // build forward paths
        HashMap<String, Paths> endNodeToItsForwardLinks = new HashMap<String, Paths>();
        parentNodes.clear();
        parentNodes.add(src);

        while(!parentNodes.isEmpty() && maxDepth-- > -1) {
            HashSet<String> newParents = new HashSet<String>();
            for (String parent: parentNodes) {

                boolean hasBackwardLink = endNodeToItsBackwardLinks.containsKey(parent) && endNodeToItsBackwardLinks.get(parent).getPaths() != null;
                boolean hasForwardLink = neighbours.containsKey(parent);

                // foreach child, add endNodeLink
                if (!hasBackwardLink && !hasForwardLink) {
                    continue;
                }

                if (hasBackwardLink) {
                    Paths parentPaths = endNodeToItsForwardLinks.get(parent);
                    Paths dstPaths;
                    if(endNodeToItsForwardLinks.containsKey(dst)) {
                        dstPaths = endNodeToItsForwardLinks.get(dst);
                    } else {
                        dstPaths = new Paths();
                    }

                    for (Path backwardPathFromDstToParent : endNodeToItsBackwardLinks.get(parent).getPaths()) {
                        for (Path path : parentPaths.getPaths()) {
                            dstPaths.addPath(path.clone().addAll(backwardPathFromDstToParent.getNodes()));
                        }
                        endNodeToItsForwardLinks.put(dst, dstPaths);
                    }
                }

                if(hasForwardLink) {
                    for (Tuple childTuple : neighbours.get(parent)) {

                        String child = childTuple.getString(toField);
                        newParents.add(childTuple.getString(toField));
                        if (endNodeToItsForwardLinks.containsKey(child)) {
                            Paths parentPaths = endNodeToItsForwardLinks.get(parent);
                            Paths childPaths = endNodeToItsForwardLinks.get(child);
                            if(parentPaths == null) {
                                childPaths.addPath(new Path().add(childTuple));
                            } else {
                                for (Path path : parentPaths.getPaths()) {
                                    childPaths.addPath(path.clone().add(childTuple));
                                }
                            }
                        } else {
                            if (endNodeToItsForwardLinks.containsKey(parent)) {
                                Paths parentPaths = endNodeToItsForwardLinks.get(parent);
                                Paths childPaths = new Paths();
                                for (Path path : parentPaths.getPaths()) {
                                    childPaths.addPath(path.clone().add(childTuple));
                                }
                                endNodeToItsForwardLinks.put(childTuple.getString(toField), childPaths);
                            } else {
                                // adding only child now. Earlier it was (parent, child)
                                Path p = Path.getInstance(Stream.of(childTuple)
                                        .collect(Collectors.toCollection(ArrayList<Tuple>::new)));
                                endNodeToItsForwardLinks.put(childTuple.getString(toField), Paths.getInstance(p));
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

        Paths paths = endNodeToItsForwardLinks.get(dst);
        System.out.println(paths);
/*
        HashMap<String, HashSet<String>> parentToChildConn = getBGNeighbours();
        String src = "billgates";
        String dst = "msft";
*/

        /*Integer maxDepth = 10;
        HashMap<String, Paths> endNodeToItsLinks = new HashMap<String, Paths>();
        Set<String> parentNodes = new HashSet<String>();
        Set<String> allVisited = new HashSet<String>();
        parentNodes.add(src);

        while(!parentNodes.isEmpty() && maxDepth-- > -1) {
            allVisited.addAll(parentNodes);
            Set<String> newParents = new HashSet<String>();
            for (String parent: parentNodes) {
                // foreach child, add endNodeLink
                if(!parentToChildConn.containsKey(parent)) {
                    continue;
                }
                newParents.addAll(parentToChildConn.get(parent));
                for (String child: parentToChildConn.get(parent)) {

                    if(endNodeToItsLinks.containsKey(child)) {
                        Paths parentPaths = endNodeToItsLinks.get(parent);
                        Paths childPaths = endNodeToItsLinks.get(child);
                        for (Path path: parentPaths.getPaths()) {
                            childPaths.addPath(path.clone().add(child));
                        }
                    } else {
                        if(endNodeToItsLinks.containsKey(parent)) {
                            Paths parentPaths = endNodeToItsLinks.get(parent);
                            Paths childPaths = new Paths();
                            for (Path path: parentPaths.getPaths()) {
                                childPaths.addPath(path.clone().add(child));
                            }
                            endNodeToItsLinks.put(child, childPaths);
                        }
                        else {
                            Path p = Path.getInstance(Stream.of(parent, child)
                                    .collect(Collectors.toCollection(ArrayList::new)));
                            endNodeToItsLinks.put(child, Paths.getInstance(p));
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


        System.out.println(endNodeToItsLinks.get(dst));*/
    }

    private static HashMap<String, HashSet<String>> getNeighbours() {
        HashMap<String, HashSet<String>> neighbourNodes = new HashMap<String, HashSet<String>>();
        neighbourNodes.put("einstein", Stream.of("birth_place1", "place_of_birth1")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("birth_place1", Stream.of("ulm")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("place_of_birth1", Stream.of("ulm")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("ulm", Stream.of("state1", "country1")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("state1", Stream.of("baden_wurttemburg")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("baden_wurttemburg", Stream.of("country1", "country2")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("country1", Stream.of("germany")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("country2", Stream.of("germany")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("stuffgart", Stream.of("capital1, state1")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("capital1", Stream.of("baden_wurttemburg")
                .collect(Collectors.toCollection(HashSet::new)));

        return neighbourNodes;
    }

    private static HashMap<String, List<Tuple>> getBGNeighbours1() {
        HashMap<String, List<Tuple>> neighbourNodes = new HashMap<String, List<Tuple>>();
        Tuple t = new Tuple();
        t.put("src_s", "billgates");
        t.put("relation_s", "cofounder");
        t.put("dst_s", "microsoft");

        Tuple t1 = create("bg","founder", "ms");
        Tuple t2 = create("bg","birth_place", "us");
        Tuple t3 = create("ms","located_in", "us");
        Tuple t4 = create("bg","developed", "windows");
        // Tuple t1 = create("windows","product_of", "ms");
        Tuple t5 = create("ms","product", "windows");
        Tuple t6 = create("windows","written_in",".net");
        Tuple t7 = create("bg","has_skill",".net");
//        Tuple t1 = create("bg","founder", "ms");
//        Tuple t1 = create("bg","founder", "ms");

        List<Tuple> l = new ArrayList<Tuple>();
        l.add(t1);
        l.add(t2);
        l.add(t4);
        l.add(t7);

        List<Tuple> l2 = new ArrayList<Tuple>();
        l2.add(t3);
        l2.add(t5);

        List<Tuple> l3 = new ArrayList<Tuple>();
        l3.add(t6);

        neighbourNodes.put("bg", l);
        neighbourNodes.put("ms", l2);
        neighbourNodes.put("windows", l3);

        return neighbourNodes;
    }

    private static Tuple create(String src, String relation, String dst) {
        Tuple t = new Tuple();
        t.put("src_s", src);
        t.put("relation_s", relation);
        t.put("dst_s", dst);
        return t;
    }

    private static HashMap<String, HashSet<String>> getBGNeighbours() {
        HashMap<String, HashSet<String>> neighbourNodes = new HashMap<String, HashSet<String>>();

        neighbourNodes.put("billgates", Stream.of("marriage1", "located1", "cofounder1", "workedon1")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("marriage1", Stream.of("melindagates")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("located1", Stream.of("us")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("cofounder1", Stream.of("msft")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("workedon1", Stream.of("msdos")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("melindagates", Stream.of("member1", "located2")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("member1", Stream.of("msft")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("located2", Stream.of("us")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("us", Stream.of("organization1")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("organization1", Stream.of("msft")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("msdos", Stream.of("developedat1", "productof1")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("developedat1", Stream.of("us")
                .collect(Collectors.toCollection(HashSet::new)));

        neighbourNodes.put("productof1", Stream.of("msft")
                .collect(Collectors.toCollection(HashSet::new)));

        return neighbourNodes;
    }
}
