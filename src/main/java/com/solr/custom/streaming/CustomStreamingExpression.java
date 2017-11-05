/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.solr.custom.streaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.*;

public class CustomStreamingExpression extends TupleStream implements Expressible {

  private boolean sendEof = false;
  private StreamContext context;
  private StreamExpression expression;
  private StreamFactory factory;

  public CustomStreamingExpression(StreamExpression expression, StreamFactory factory) throws IOException{
    this.expression = expression;
    this.factory = factory;
  }

  @Override
  public Map toMap(final Map<String, Object> map) {
    return super.toMap(map);
  }

  @Override
  public void setStreamContext(StreamContext context) {
    this.context = context;
  }

  @Override
  public List<TupleStream> children() {
    return null;
  }

  @Override
  public void open() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Tuple read() throws IOException {

    Map<String, Object> m = new HashMap<>();
    if (sendEof) {
      m.put("EOF", true);
    } else {
      sendEof = true;
      m.put("msg", "Hello! Custom Streaming Expression");
    }
    return new Tuple(m);
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
        .withFunctionName("customstreamingexpression")
        .withImplementingClass(this.getClass().getName())
        .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
        .withExpression("--non-expressible--");
  }
}