package com.solr.custom.streaming;

import com.solr.custom.streaming.*;
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

        HashMap<String, HashSet<String>> parentToChildConn = getNeighbours();
        String src = "einstein";
        String dst = "germany";

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
