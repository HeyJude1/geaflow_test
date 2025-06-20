/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 package com.antgroup.geaflow.dsl.udf.graph;

 import com.antgroup.geaflow.common.tuple.Tuple;
 import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
 import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
 import com.antgroup.geaflow.dsl.common.data.Row;
 import com.antgroup.geaflow.dsl.common.data.RowEdge;
 import com.antgroup.geaflow.dsl.common.data.RowVertex;
 import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
 import com.antgroup.geaflow.dsl.common.function.Description;
 import com.antgroup.geaflow.dsl.common.types.GraphSchema;
 import com.antgroup.geaflow.dsl.common.types.StructType;
 import com.antgroup.geaflow.dsl.common.types.TableField;
 import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
 import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
 
 import java.util.HashSet;
 import java.util.Iterator;
 import java.util.List;
 import java.util.Optional;
 
 @Description(name = "common_neighbors_for_vertex_set", description = "built-in udga for CommonNeighborsForVertexSet")
 public class CommonNeighborsForVertexSet implements AlgorithmUserFunction<Object, Tuple<Boolean, Boolean>> {
     
     private AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context;
     private HashSet<Object> A = new HashSet<>(); // 存储A集合
     private HashSet<Object> B = new HashSet<>(); // 存储B集合
 
     @Override
     public void init(AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context, Object[] params) {
         this.context = context;
         
         // 解析参数：params[0]是A集合的第一个元素，params[1]是分隔符，params[2]开始是B集合的元素
         // 根据SQL调用：CALL common_neighbors_for_vertex_set(3, '|', 2, 5)
         // params[0] = 3 (A集合的元素)
         // params[1] = "|" (分隔符，用于区分A和B集合)
         // params[2] = 2, params[3] = 5 (B集合的元素)
         
         if (params.length >= 3) {
             // 添加A集合的元素（分隔符之前的元素）
             A.add(TypeCastUtil.cast(params[0], context.getGraphSchema().getIdType()));
             
             // 从分隔符后开始添加B集合的元素
             for (int i = 2; i < params.length; i++) {
                 B.add(TypeCastUtil.cast(params[i], context.getGraphSchema().getIdType()));
             }
         }
     }
 
     @Override
     public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Boolean, Boolean>> messages) {
         Object vertexId = vertex.getId();
         
         if (context.getCurrentIterationId() == 1L) {
             // 第一轮：A和B集合中的顶点向邻居发送标识消息
             Tuple<Boolean, Boolean> messageToSend = new Tuple<>(false, false);
             
             // 检查当前顶点是否属于A集合或B集合
             if (A.contains(vertexId)) {
                 messageToSend.setF0(true); // 标识来自A集合
             }
             if (B.contains(vertexId)) {
                 messageToSend.setF1(true); // 标识来自B集合
             }
             
             // 只有属于A或B集合的顶点才发送消息
             if (messageToSend.getF0() || messageToSend.getF1()) {
                 List<RowEdge> edges = context.loadEdges(EdgeDirection.BOTH);
                 sendMessageToNeighbors(edges, messageToSend);
             }
             
         } else if (context.getCurrentIterationId() == 2L) {
             // 第二轮：检查是否同时收到A和B的消息
             Tuple<Boolean, Boolean> received = new Tuple<>(false, false);
             
             // 解析收到的其他顶点的消息
             while (messages.hasNext()) {
                 Tuple<Boolean, Boolean> message = messages.next();
                 if (message.getF0()) {
                     received.setF0(true); // 收到来自A集合的消息
                 }
                 if (message.getF1()) {
                     received.setF1(true); // 收到来自B集合的消息
                 }
             }
             
             // 如果同时收到A和B的消息，则为共同邻居
             if (received.getF0() && received.getF1()) {
                 context.take(ObjectRow.create(vertex.getId()));
             }
         }
     }
 
     @Override
     public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
         // 算法结束时的清理工作（可选）
     }
 
     @Override
     public StructType getOutputType(GraphSchema graphSchema) {
         return new StructType(
             new TableField("id", graphSchema.getIdType(), false)
         );
     }
 
     /**
      * 向邻居顶点发送消息的辅助方法
      */
     private void sendMessageToNeighbors(List<RowEdge> edges, Tuple<Boolean, Boolean> message) {
         for (RowEdge rowEdge : edges) {
             context.sendMessage(rowEdge.getTargetId(), message);
         }
     }
 }