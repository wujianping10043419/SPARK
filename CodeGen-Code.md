#Filter
```
public Object generate(Object[] references) {
  return new GeneratedIterator(references);
}


final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator inputadapter_input;
  private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows;
  private UnsafeRow filter_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter;
  private org.apache.spark.broadcast.TorrentBroadcast bhj_broadcast;
  private org.apache.spark.sql.execution.joins.UnsafeHashedRelation bhj_relation;
  private UnsafeRow bhj_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder bhj_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter bhj_rowWriter;
  private org.apache.spark.sql.execution.metric.SQLMetric bhj_numOutputRows;
  private UnsafeRow bhj_result1;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder bhj_holder1;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter bhj_rowWriter1;
  private UnsafeRow project_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter;

  public GeneratedIterator(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator inputs[]) {
    partitionIndex = index;
    inputadapter_input = inputs[0];
    this.filter_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
    filter_result = new UnsafeRow(2);
    this.filter_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result, 64);
    this.filter_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder, 2);
    this.bhj_broadcast = (org.apache.spark.broadcast.TorrentBroadcast) references[1];
    bhj_relation = ((org.apache.spark.sql.execution.joins.UnsafeHashedRelation) bhj_broadcast.value()).asReadOnlyCopy();
    incPeakExecutionMemory(bhj_relation.estimatedSize()); 
    bhj_result = new UnsafeRow(1);
    this.bhj_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(bhj_result, 32);
    this.bhj_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(bhj_holder, 1);
    this.bhj_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[2];
    bhj_result1 = new UnsafeRow(4);
    this.bhj_holder1 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(bhj_result1, 128);
    this.bhj_rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(bhj_holder1, 4);
    project_result = new UnsafeRow(3);
    this.project_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result, 96);
    this.project_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder, 3);
  }  

  protected void processNext() throws java.io.IOException {
    while (inputadapter_input.hasNext()) {
      InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
      boolean inputadapter_isNull = inputadapter_row.isNullAt(0);
      UTF8String inputadapter_value = inputadapter_isNull ? null : (inputadapter_row.getUTF8String(0));
      if (!(!(inputadapter_isNull))) continue;       
      filter_numOutputRows.add(1);
      // generate join key for stream side
      bhj_holder.reset();        
      bhj_rowWriter.write(0, inputadapter_value);
      bhj_result.setTotalSize(bhj_holder.totalSize());      
      // find matches from HashedRelation
      UnsafeRow bhj_matched = bhj_result.anyNull() ? null: (UnsafeRow)bhj_relation.getValue(bhj_result);
      if (bhj_matched == null) continue;

      bhj_numOutputRows.add(1);
      boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
      UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
      boolean bhj_isNull2 = bhj_matched.isNullAt(1);
      UTF8String bhj_value2 = bhj_isNull2 ? null : (bhj_matched.getUTF8String(1));
      project_holder.reset();        
      project_rowWriter.zeroOutNullBytes();      
      project_rowWriter.write(0, inputadapter_value);      
      if (inputadapter_isNull1) {
        project_rowWriter.setNullAt(1);
      } else {
        project_rowWriter.write(1, inputadapter_value1);
      }
      if (bhj_isNull2) {
        project_rowWriter.setNullAt(2);
      } else {
        project_rowWriter.write(2, bhj_value2);
      }
      project_result.setTotalSize(project_holder.totalSize());
      append(project_result);
      if (shouldStop()) return;
    }
  }
}
```



#Join
```
insert overwrite table peopleWithId SELECT a.name, a.age, b.id FROM  people2 a join id2 b on a.name = b.name

public Object generate(Object[] references) {
  return new GeneratedIterator(references);
}
/*wholestagecodegen_c1*/
final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator inputadapter_input;
  private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows;
  private org.apache.spark.sql.execution.metric.SQLMetric filter_filterTimeCodegen;
  private UnsafeRow filter_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter;
  private org.apache.spark.broadcast.TorrentBroadcast bhj_broadcast;
  private org.apache.spark.sql.execution.joins.UnsafeHashedRelation bhj_relation;
  private UnsafeRow bhj_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder bhj_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter bhj_rowWriter;
  private org.apache.spark.sql.execution.metric.SQLMetric bhj_numOutputRows;
  private org.apache.spark.sql.execution.metric.SQLMetric bhj_BroadcastHashJoinExec_time_codegen_match;
  private org.apache.spark.sql.execution.metric.SQLMetric bhj_BroadcastHashJoinExec_time_Inner;
  private UnsafeRow bhj_result1;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder bhj_holder1;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter bhj_rowWriter1;
  private org.apache.spark.sql.execution.metric.SQLMetric project_projectTimeCodegen;
  private UnsafeRow project_result;
  private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter;
  public GeneratedIterator(Object[] references) {
    this.references = references;
  }
  public void init(int index, scala.collection.Iterator inputs[]) {
    partitionIndex = index;
    inputadapter_input = inputs[0];
    this.filter_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
    this.filter_filterTimeCodegen = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
    filter_result = new UnsafeRow(2);
    this.filter_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result, 64);
    this.filter_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder, 2);
    this.bhj_broadcast = (org.apache.spark.broadcast.TorrentBroadcast) references[2];
     bhj_relation = ((org.apache.spark.sql.execution.joins.UnsafeHashedRelation) bhj_broadcast.value()).asReadOnlyCopy();
     incPeakExecutionMemory(bhj_relation.estimatedSize());
           
    bhj_result = new UnsafeRow(1);
    this.bhj_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(bhj_result, 32);
    this.bhj_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(bhj_holder, 1);
    this.bhj_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[3];
    this.bhj_BroadcastHashJoinExec_time_codegen_match = (org.apache.spark.sql.execution.metric.SQLMetric) references[4];
    this.bhj_BroadcastHashJoinExec_time_Inner = (org.apache.spark.sql.execution.metric.SQLMetric) references[5];
    bhj_result1 = new UnsafeRow(4);
    this.bhj_holder1 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(bhj_result1, 128);
    this.bhj_rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(bhj_holder1, 4);
    this.project_projectTimeCodegen = (org.apache.spark.sql.execution.metric.SQLMetric) references[6];
    project_result = new UnsafeRow(3);
    this.project_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result, 96);
    this.project_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder, 3);
  }
  
  protected void processNext() throws java.io.IOException {
    /*project_c*/
    /*bhj_c*/
    /*filter_c*/
    /*inputadapter_c*/
     while (inputadapter_input.hasNext() && !stopEarly()) {
      InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
      /*filter_c1*/
      long filter_beforeFilter = System.nanoTime();
      /*inputadapter_c1*/
      boolean inputadapter_isNull = inputadapter_row.isNullAt(0);
      UTF8String inputadapter_value = inputadapter_isNull ? null : (inputadapter_row.getUTF8String(0));
      if (!(!(inputadapter_isNull))) continue;

      filter_numOutputRows.add(1);
      filter_filterTimeCodegen.add((System.nanoTime() - filter_beforeFilter));
      /*bhj_c1*/
      // generate join key for stream side
      bhj_holder.reset();      
      bhj_rowWriter.write(0, inputadapter_value);
      bhj_result.setTotalSize(bhj_holder.totalSize());            
      // find matches from HashedRelation
      long bhj_innerBegin = System.nanoTime();
      UnsafeRow bhj_matched = bhj_result.anyNull() ? null: (UnsafeRow)bhj_relation.getValue(bhj_result);
      bhj_BroadcastHashJoinExec_time_codegen_match.add(System.nanoTime() - bhj_innerBegin);
      if (bhj_matched == null) continue;
      bhj_BroadcastHashJoinExec_time_Inner.add(System.nanoTime() - bhj_innerBegin);
      bhj_numOutputRows.add(1);
      /*project_c1*/
      long project_beforePro = System.nanoTime();
      project_projectTimeCodegen.add((System.nanoTime() - project_beforePro));
      /*wholestagecodegen_c*/
      /*project_c2*/
      /*inputadapter_c2*/
      boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
      UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
      /*project_c3*/
      /*bhj_c3*/
      boolean bhj_isNull2 = bhj_matched.isNullAt(1);
      UTF8String bhj_value2 = bhj_isNull2 ? null : (bhj_matched.getUTF8String(1));
      project_holder.reset();
      project_rowWriter.zeroOutNullBytes(); 
      project_rowWriter.write(0, inputadapter_value);
      if (inputadapter_isNull1) {
        project_rowWriter.setNullAt(1);
      } else {
        project_rowWriter.write(1, inputadapter_value1);
      }      
      if (bhj_isNull2) {
        project_rowWriter.setNullAt(2);
      } else {
        project_rowWriter.write(2, bhj_value2);
      }
      project_result.setTotalSize(project_holder.totalSize());
      append(project_result);
      if (shouldStop()) return;
    }
  }
}
```
