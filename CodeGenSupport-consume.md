
INSERT OVERWRITE TABLE id3 select name, id from id2 where name is not null

# Produce：  
入参：  outputVars   输出变量
        row          输入的行

# consume：  
入参：  outputVars   输出变量
        row          输入的行

consume的2个入参比较有趣，当outputVars为null的时候，需要计算当前Exec的output，并绑定index后创建代码生成inputVars（2个ExprCode）此时row字符串为变量名；如InputAdapter。
                        当


## InputAdapter：

    consume：
        outputVars = null
        row = "inputadapter_row"


```
val inputVars = ExprCode(boolean inputadapter_isNull = inputadapter_row.isNullAt(0);UTF8String inputadapter_value = inputadapter_isNull ? null : (inputadapter_row.getUTF8String(0));
                        ,inputadapter_isNull
                        ,inputadapter_value)
                ExprCode(boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                	    ,inputadapter_isNull1
                	    ,inputadapter_value1)

val rowVar = ExprCode(,false,inputadapter_row)
```




## FilterExec：

    doConsumer:

```
//genPredicate根据判null表达式匹配，将第0变量生成“如果为null则continue”的代码；并且将入参的input（其实就是上一级inputVars）的第0变量的code段置空
// Expression c = isnotnull(name#4)
// val bound = isnotnull(input[0, string, true])
// val evaluated = boolean inputadapter_isNull = inputadapter_row.isNullAt(0);
//                 UTF8String inputadapter_value = inputadapter_isNull ? null : (inputadapter_row.getUTF8String(0));      
// val ev = ExprCode(,false,(!(inputadapter_isNull)))
val nullChecks = boolean inputadapter_isNull = inputadapter_row.isNullAt(0);
                 UTF8String inputadapter_value = inputadapter_isNull ? null : (inputadapter_row.getUTF8String(0));
                 if (!(!(inputadapter_isNull))) continue;


val input = ExprCode(,inputadapter_isNull,inputadapter_value)
            ExprCode(boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                    ,inputadapter_isNull1
               	    ,inputadapter_value1)       



//根据判null表达式匹配，将第0变量的isNull段置false
val resultVars = ExprCode(,false,inputadapter_value)
                 ExprCode(boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                 	,inputadapter_isNull1
                 	,inputadapter_value1)
```




最后：generated为“”，所以返回的是nullChecks和后续的consume

```
    s""“
       |$generated
       |$nullChecks
       |$numOutput.add(1);
       |${consume(ctx, resultVars)}
     ""”.stripMargin
```



    consume： 
       outputVars = resultVars
       row = null

```
val inputVars = ExprCode(,false,inputadapter_value)
                ExprCode(boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                 	,inputadapter_isNull1
                 	,inputadapter_value1)

//获取output表达式
val colExprs = input[0, string, false]
               input[1, string, true]

//获取inputVars的非空code部分，并且将outputVars的code清空，注意， inputVars是outputVars的copy             
val evaluateInputs = boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
                     UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                     
// 调用GenerateUnsafeProjection生成写unsafeRow的代码，需要colExprs，这里把这个流程放在求evaluateInputs之后，应该是因为cxt需要更新后的outputVars
//尝试将GenerateUnsafeProjection的writeExpressionsToBuffer生成的代码加上“|”
val ev = ExprCode(
        filter_holder.reset();
        filter_rowWriter.zeroOutNullBytes();
            filter_rowWriter.write(0, inputadapter_value);
            if (inputadapter_isNull1) {
              filter_rowWriter.setNullAt(1);
            } else {
              filter_rowWriter.write(1, inputadapter_value1);
            }
        filter_result.setTotalSize(filter_holder.totalSize());
      ,false
      ,filter_result)                     

val rowVar = ExprCode(boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
                      UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                      filter_holder.reset();
                      filter_rowWriter.zeroOutNullBytes();
                      filter_rowWriter.write(0, inputadapter_value);
                      if (inputadapter_isNull1) {
                        filter_rowWriter.setNullAt(1);
                      } else {
                        filter_rowWriter.write(1, inputadapter_value1);
                      }
                      filter_result.setTotalSize(filter_holder.totalSize());,false,filter_result)
```

```
//调用GenerateUnsafeProjection生成写unsafeRow的代码过程，注册了变量定义和初始化代码，这些注册的代码，在WholeStageCodegenExec中生成init代码时被注入
cxt.mutableStates = (scala.collection.Iterator,inputadapter_input,inputadapter_input = inputs[0];)
                    (org.apache.spark.sql.execution.metric.SQLMetric,filter_numOutputRows,this.filter_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];)
                    (UnsafeRow,filter_result,filter_result = new UnsafeRow(2);)
                    (org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder,filter_holder,this.filter_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result, 64);)
                    (org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter,filter_rowWriter,this.filter_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder, 2);)
```
最后：evaluated为“”，所以返回的是父节点doConsume的结果，事实上，其父节点WholeStageCodegenExec的doConsume仅在rowVar的尾部增加了一样apply(rowVar.value),所以至此consumer的代码生成完毕。

```
    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString}")}
       |$evaluated
       |${parent.doConsume(ctx, inputVars, rowVar)}
     """.stripMargin
```

## 小结

- InputAdapter
1. 在doProduce里构造了数据输入代码，即数据源读取；
```
ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
```
2. 在doProduce里构造了读取行标识；

```
val row = ctx.freshName("row")
```

3. 在doProduce里构造了数据读取框架，即while循环；
```
    s"""
       | while ($input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${consume(ctx, null, row).trim}
       |   if (shouldStop()) return;
       | }
     """.stripMargin
```
4. 在consumer，根据output生成了各字段的读取代码，暂时该代码仅存在表达式中；
```
val inputVars = ExprCode(boolean inputadapter_isNull = inputadapter_row.isNullAt(0);UTF8String inputadapter_value = inputadapter_isNull ? null : (inputadapter_row.getUTF8String(0));
                        ,inputadapter_isNull
                        ,inputadapter_value)
                ExprCode(boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                	    ,inputadapter_isNull1
                	    ,inputadapter_value1)

val rowVar = ExprCode(,false,inputadapter_row)
```

- FilterExec
1. FilterExec的doConsumer方法里，首先匹配到过滤表达式字段，从入参里查到对应的字段读取代码，并附加如果不成立则continue的逻辑（因为这些代码必然嵌入到while中）
```
val nullChecks = boolean inputadapter_isNull = inputadapter_row.isNullAt(0);
                 UTF8String inputadapter_value = inputadapter_isNull ? null : (inputadapter_row.getUTF8String(0));
                 if (!(!(inputadapter_isNull))) continue;
```
2. FilterExec的doConsumer方法里，将inputVars里各字段的code字段设置为空，且不为空表达式对应的isNull字段设置为false；
```
//这里generated为空
    s"""
       |$generated
       |$nullChecks
       |$numOutput.add(1);
       |${consume(ctx, resultVars)}
     """.stripMargin
```

3. 在consumer，先用evaluateVariables提取了非过滤字段的赋值代码；
```
val evaluateInputs = boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
                     UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
```
4. 然后用GenerateUnsafeProjection生成了unsafeRow的写代码ev；
```
val ev = ExprCode(
        filter_holder.reset();
        filter_rowWriter.zeroOutNullBytes();
            filter_rowWriter.write(0, inputadapter_value);
            if (inputadapter_isNull1) {
              filter_rowWriter.setNullAt(1);
            } else {
              filter_rowWriter.write(1, inputadapter_value1);
            }
        filter_result.setTotalSize(filter_holder.totalSize());
      ,false
      ,filter_result)  
```

5. 然后合并成和evaluateVariables一起合并成rowVar
```
val rowVar = ExprCode(boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
                      UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                      filter_holder.reset();
                      filter_rowWriter.zeroOutNullBytes();
                      filter_rowWriter.write(0, inputadapter_value);
                      if (inputadapter_isNull1) {
                        filter_rowWriter.setNullAt(1);
                      } else {
                        filter_rowWriter.write(1, inputadapter_value1);
                      }
                      filter_result.setTotalSize(filter_holder.totalSize());,false,filter_result)
```
6. 该代码在表达式中传递给parent的doConsume；
```
//这里evaluated为空
    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString}")}
       |$evaluated
       |${parent.doConsume(ctx, inputVars, rowVar)}
     """.stripMargin
```
## WholeStageCodegenExec
WholeStageCodegenExec的doConsumer仅仅在rowVar尾部增加了apply。
```
    s"""
      |${row.code}
      |append(${row.value}$doCopy);
     """.stripMargin.trim
```

所以，整个consumer过程：
1. InputAdapter在produce生成数据源以及循环读取框架；
2. InputAdapter在consumer生成各字段的读取代码，保存在表达式中；
3. FilterExec在doConsumer中生成过滤字段的读取并continue逻辑；
4. FilterExec在consumer中生成非过滤字段的读取代码和unsafeRow写入代码，保存在表达式中；
5. WholeStageCodegenExec的doConsumer在FilterExec的row代码后增加apply操作。


