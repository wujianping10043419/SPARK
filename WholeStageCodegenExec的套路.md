#WholeStageCodegenExec的套路

我们用一个简单的例子实践WholeStageCodegenExec的codegen过程：
```
insert overwrite table id3 select name,id from id2 where id is not null
== Physical Plan ==
InsertIntoHiveTable MetastoreRelation default, id3, true, false
+- *Filter isnotnull(id#11)
   +- HiveTableScan [name#10, id#11], MetastoreRelation default, id2
```
##CollapseCodegenStages

 org.apache.spark.sql.execution.QueryExecution的preparations包含了CollapseCodegenStages策略.该策略应用于sparkPlan转换executedPlan过程。
CollapseCodegenStages的apply方法，当conf.wholeStageEnabled为真则调用插入WholeStageCodegen，否则直接返回plan。
当打开spark.sql.codegen.wholeStage开关时（默认打开），此策略往物理计划里添加InputAdapter和WholeStageCodegen。

如下2节，insertWholeStageCodegen和insertInputAdapter配合形成一个递归。总结规则就是：

从**insertWholeStageCodegen**开始：
1. 从plan根开始，检查是否需要插入WholeStageCodegen，如果不满足插入WholeStageCodegen条件，则对所有子节点做**insertWholeStageCodegen**检查；这是最简单递归，如果所有物理计划节点都不支持，则说明都没干，如果有，则变更子树；
2. 如果某个节点支持Codegen，则要在此处插入WholeStageCodegen；但首先对所有子树执行检查是否需要插入InputAdapter（**insertInputAdapter**）；即WholeStageCodegen的子树才需要检查是否插入InputAdapter；
3. 插入InputAdapter的条件是节点不支持Codegen或者本节点是SortMergeJoinExec（后者可以理解是特例，它对左右子节点执行**insertWholeStageCodegen**检查，并且在前面插入InputAdapter替换为新的left和right）；
4. 如果不满足插入InputAdapter条件，则对其检查子节点是否需要插入InputAdapter（**insertInputAdapter**），最后到叶节点一定会满足条件的（这里不对，叶节点可以支持Codegen，比如DataScanExec）直到叶节点。

这是一个很有意思的交叉递归：
1. 如果满足插入WholeStageCodegen条件，则对子节点insertInputAdapter，然后插入；否则向下检查；
2. 如果满足插入InputAdapter条件，则对子节点insertWholeStageCodegen，然后插入；否则向下检查；
3. 满足插入InputAdapter的条件是：不支持Codegen；例外是SortMergeJoinExec节点；
4. 满足插入WholeStageCodegen的条件是：支持Codegen；例外是输出为单个ObjectType节点；
由于成立条件相反，又交叉调用；所以，结果是用WholeStageCodegen和InputAdapter分隔链路上支持和不支持Codegen的节点。如下：
- W:WholeStageCodegen
- C:Plan_Codegen
- I:InputAdapter
- N:Plan_NoCodegen

| W | C | C | C | I | N | N | N | N | W | C |
|:--|--:|:--|:--|--:|:--|:--|--:|:--|:--|:--|
| W	| C | C | I | N	| N | N | W | C | I	| N |

###insertWholeStageCodegen

insertWholeStageCodegen(plan)：
1. 如果plan的output.length为1，且类型为ObjectType时，则对它所有子节点执行insertWholeStageCodegen；然后对返回的物理计划执行子节点替换(WithNewChildren)，这里是一个递归；
2. 如果plan是一个CodegenSupport类型，且supportCodegen为真(此方法检查了是否有不支持Codegen的表达式，输入输出文件是否超出限制)，则对本plan执行insertInputAdapter，然后插入WholeStageCodegenExec；
3. 其他情况则对它所有子节点执行insertWholeStageCodegen；然后对返回的物理计划执行子节点替换(WithNewChildren)；这里处理和第1步一样，什么场景不能和3合并呢?

###insertInputAdapter(plan)：
1. 如果plan是SortMergeJoinExec且支持Codegen，则对left和right执行insertWholeStageCodegen(讨厌的嵌套递归)，然后在外面插入InputAdapter；
2. 如果不支持Codegen，对它所有子节点执行insertWholeStageCodegen；然后在外面插入InputAdapter；
3. 给它所有子节点执行insertInputAdapter，然后对返回的物理计划执行子节点替换(WithNewChildren)。

##CodegenSupport

- 这是一个特质，支持代码生成的物理操作符的接口，没有具体的构造成员：parent：在produce的时候赋值，在consume的时候使用；
- doProduce\Produce\doConsume\Consume\inputRDDs为关键方法
- evaluateRequiredVariables\evaluateVariables\usedInputs辅助代码生成

分析CodegenSupport的继承类，总计17个case class和1个trait(被2个case class继承)，分析其对以上5个关键方法的重载：
1. 其中，inputRDDs为一个Codegen链的输入，见inputRDDs；
2. doProduce\Produce\doConsume\Consume是一个代码生成体系，支持Codegen的物理计划节点的这些函数构成一个递归调用链，最后生成一个java函数，见Produce开始的4节
![ Produce\Consume体系图]()
![Alt text](./page_1.png)


###inputRDDs
从这里18个CodengenSupport的物理计划，inputRDDs方法分3类：
1. 通用算法，即读取child的inputRDDs；
2. 三个自定义的inputRDDs，是自己生成RDD[InternalRow]，如RangeExec；或者本身包含RDD。即其本身就是叶子节点；
3. InputAdapter的算法是child.execute()，由InputAdapter的生成过程指导，其是“Codegen支持与否”的分界点，即其child是不支持Codegen的，所以其必须为现在的样子，同样由此知道；SortMergeJoinExec是InputAdapter的特例，SortMergeJoinExec的left和right只能是InputAdapter，其没有重载execute(),基类SparkPlan的execute调用的doExecute，而InputAdapter的此函数调用的是child.execute.所以，唉，没有所以.

所以，由上知，支持Codegen的物理计划，最终inputRDDs等于叶节点或者InputAdapter算出来的RDD。
那么这个inputRDDs被谁真正读出来呢？
在WholeStageCodegenExec的doExecute()方法中，一些列的代码生成后，其读child的inputRDDs作为数据源，应用到生成的转换代码。
	所以，inputRDDs是被WholeStageCodegenExec读取，其通过链接子树的inputRDDs迭代到叶子节点方向第一个InputAdapter或者结尾；然后被WholeStageCodegenExec作为数据源，应用到生成的转换代码。
这里的意思是WholeStage生成的代码是此WholeStageCodegenExec开始的Codegen链所有物理计划的代码和是吧。

|class|default|define|exception|
|:--:|:--:|:--:|:--:|
|ProjectExec|child|||
|FilterExec|child|||
|SampleExec|child|||
|RangeExec||RDD||
|RowDataSourceScanExec||RDD||
|BatchedDataSourceScanExec||RDD||
|ExpandExec|child|||
|DeserializeToObjectExec|child|||
|SerializeFromObjectExec|child|||
|MapElementsExec|child|||
|SortExec|child|||
|InputAdapter|child.execute()||child.execute()|
|WholeStageCodegenExec|exception||exception|
|HashAggregateExec|child|||
|DebugExec|child|||
|BroadcastHashJoinExec|child|||
|SortMergeJoinExec|||left.execute()::right.execute()|
|BaseLimitExec||||
|(Local\Global)|child|||


###Produce

这个方法的定义注释说：返回处理来自输入RDD的行的Java源代码。
有意思的地方在于，这个方法没有人重载，即所有Exec都是用的同样的算法，该方法调用了自己的doProduce。
该方法实现了非常简单的逻辑，除了调用自己的doProduce，只有2个简单功能：
1. 因为其总是被物理计划链的父节点调用，所以入参的父节点指针被赋给当前节点的parent成员；即构建从下向上链路结构；
2. 将当前物理计划的名称前缀赋值给ctx。
其中注释代码部分无逻辑意义，可以忽略。
```
Returns Java source code to process the rows from input RDD.
final def produce(ctx: CodegenContext, parent: CodegenSupport): String = executeQuery {
  this.parent = parent
  ctx.freshNamePrefix = variablePrefix
  s"""
     |${ctx.registerComment(s"PRODUCE: ${this.simpleString}")}
     |${doProduce(ctx)}
   """.stripMargin
}
```


那么，该方法被谁调用呢？
所有调用的地方都是相同的代码，显然都是其父节点调用了它：
```
child.asInstanceOf[CodegenSupport].produce(ctx, this)
```
该方法在14个地方被调用，其实是13个Exec中被调用，HashAggregateExec有2个分支场景；
1. 从调用位置看，下面default列的10个Exec的doProduce方法直接调用了子节点的Produce；
2. SortExec和HashAggregateExec有比较复杂的doProduce代码创建过程，但其同样调用了子节点的Produce；
3. WholeStageCodegenExec是在其doCodeGen方法调用了子节点的Produce；
4. 和inputRDDs类似，3个自定义的inputRDDs的Exec，还有InputAdapter，以及SortMergeJoinExec是没有调用child的doProduce，他们包含了复杂的代码生成过程；其中SortMergeJoinExec稍有疑问，道理上讲，它子节点是inputAdapter，是可以调用子节点的Produce的，可能是因为其本身应该有复杂的代码生成过程，所以跳过了inputAdapter吧。由于暂时没有此场景用例，暂不分析，做为特例吧。

|class|default|define|exception|
|:--:|:--:|:--:|:--:|
|ProjectExec|child|　|　|
|FilterExec|child|　|　|
|SampleExec|child|　|　|
|RangeExec|　|　|　|
|RowDataSourceScanExec|　|　|　|
|BatchedDataSourceScanExec|　|　|　|
|ExpandExec|child|　|　|
|DeserializeToObjectExec|child|　|　|
|SerializeFromObjectExec|child|　|　|
|MapElementsExec|child|　|　|
|SortExec|　|SortExec|　|
|InputAdapter|　|　|　|
|WholeStageCodegenExec|　||exception|
|HashAggregateExec|　|HashAggregateExec|　|
|DebugExec|child|　|　|
|BroadcastHashJoinExec|child||　|
|SortMergeJoinExec|　|　|　|
|BaseLimitExec|
|(Local\Global)|child|　|　|

所以，由上知，支持Codegen的物理计划：
1. 所有Exec的Produce方法调用了自己的doProduce；然后doProduce又调用了子节点的Produce，直到Codegen链的结尾；
2. WholeStageCodegenExec是在其doCodeGen方法调用了子节点的Produce；所以这里是调用链起点；
3. RangeExec\RowDataSourceScanExec\BatchedDataSourceScanExec\InputAdapter\SortMergeJoinExec作为produce->doProduce调用的结束点，其有较复杂的代码生成过程；
4. 中间节点，只有SortExec和HashAggregateExec有教复杂的代码生成过程。

###doProduce
这个方法的定义注释说：生成要处理的Java源代码，应该被子类覆盖以支持代码生成。
```
Generate the Java source code to process, should be overridden by subclass to support codegen.
```
这个方法的实现其实在上节也描述过了，有10个物理计划是直接调用child的produce方法，有5个尾节点完全自定义(包括SortMergeJoinExec)，还有2个节点虽然调用了child的produce，但其自身也有较复杂的代码生成过程。
WholeStageCodegenExec发起点，不实现produce\doProduce；其doCodeGen方法调用了子节点的Produce。

|class|default|define|exception|
|:--:|:--:|:--:|:--:|
|ProjectExec|child|　|　|
|FilterExec|child|　|　|
|SampleExec|child|　|　|
|RangeExec|　|defined|　|
|RowDataSourceScanExec|　|defined|　|
|BatchedDataSourceScanExec|　|defined|　|
|ExpandExec|child|　|　|
|DeserializeToObjectExec|child|　|　|
|SerializeFromObjectExec|child|　|　|
|MapElementsExec|child|　|　|
|SortExec|　|SortExec|　|
|InputAdapter|　|defined|　|
|WholeStageCodegenExec|　|　|exception|
|HashAggregateExec|　|HashAggregateExec|　|
|DebugExec|child|　|　|
|BroadcastHashJoinExec|child||　|
|SortMergeJoinExec|　|SortMergeJoinExec|　|
|BaseLimitExec|
|(Local\Global)|child|　|　|

###Consume

这个方法的定义注释说：使用当前物理计划中生成的列或行，调用其父级的“doConsume()”
Consume the generated columns or row from current SparkPlan, call its parent's `doConsume()`.
同produce一样，这个方法没有人重载，即所有Exec都是用的同样的算法。此方法调用了parent.doConsume(ctx, inputVars, rowVar)
```
final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
  val inputVars =
    if (row != null) {
      ctx.currentVars = null
      ctx.INPUT_ROW = row
      output.zipWithIndex.map { case (attr, i) =>
        BoundReference(i, attr.dataType, attr.nullable).genCode(ctx)
      }
    } else {
      assert(outputVars != null)
      assert(outputVars.length == output.length)
      // outputVars will be used to generate the code for UnsafeRow, so we should copy them
      outputVars.map(_.copy())
    }
  val rowVar = if (row != null) {
    ExprCode("", "false", row)
  } else {
    if (outputVars.nonEmpty) {
      val colExprs = output.zipWithIndex.map { case (attr, i) =>
        BoundReference(i, attr.dataType, attr.nullable)
      }
      val evaluateInputs = evaluateVariables(outputVars)
      // generate the code to create a UnsafeRow
      ctx.INPUT_ROW = row
      ctx.currentVars = outputVars
      val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
      val code = s"""
        |$evaluateInputs
        |${ev.code.trim}
       """.stripMargin.trim
      ExprCode(code, "false", ev.value)
    } else {
      // There is no columns
      ExprCode("", "false", "unsafeRow")
    }
  }
  ctx.freshNamePrefix = parent.variablePrefix
  val evaluated = evaluateRequiredVariables(output, inputVars, parent.usedInputs)
  s"""
     |$evaluated
     |${parent.doConsume(ctx, inputVars, rowVar)}
   """.stripMargin
}
```
函数的前半部分用来构造inputVars和rowVar，以传递给parent.doProduce。从代码可以看得出，consume的后2个入参是互斥的，即row为空的时候outputVars被使用；否则outputVars不被使用。
- 当row为空时：
-- inputVars为outputVars
-- rowVar为:
1. 根据当前output计算BoundReference为colExprs，注意，这里output可能和子节点不一致，其根据本节点的特性修改了isNull字段；
2. 提取outputVars非空code为evaluateInputs（同时把outputVars所有code置空）；
3. 使用GenerateUnsafeProjection对colExprs生成unsafeRow的构造代码，结果ev为ExprCode(code, "false", result)，其包含代码和结果变量名称；
4. 将evaluateInputs和ev.code合并为新的代码，并构造ExprCode(code, "false", ev.value)

- 当row非空时：
-- inputVars为output（解析出要处理的字段）按序号绑定且生成的代码，包括：存在变量名，值变量名，以及从row解析“存在与否和值”的代码；生成代码的过程中，如果该字段属性可以为空，则code要包含控制判断；如果不可以，则没有。
-- rowVar为ExprCode("", "false", row) （这个ExprCode的各阶段是：code，isNull，value）

**那么谁调用了它呢?**
该方法在17个Exec中被调用,；
1. 从调用位置看，下面default列的10个Exec的Consume方法都是被自己的doConsume调用，即来自child调用；
2. 而define列的5个Exec的Consume方法都是被自己的doProduce调用，即self调用；这5个节点也是上面Produce\doProduce分析中的Codegen链的尾节点；
3. SortExec\HashAggregateExec的Consume方法也是被自己的doProduce调用，即self调用；但他们有点特殊，他们的doProduce还先调用了child.asInstanceOf[CodegenSupport].produce；
4. WholeStageCodegenExec的Consume方法没人调用，因为它是所有Codegen链的首节点，即最后一级parent。

|class|default|define|exception|
|:--:|:--:|:--:|:--:|
|ProjectExec|doConsume|　|　|
|FilterExec|doConsume|　|　|
|SampleExec|doConsume|　|　|
|RangeExec|　|doProduce|　|
|RowDataSourceScanExec|　|doProduce|　|
|BatchedDataSourceScanExec|　|doProduce|　|
|ExpandExec|doConsume|　|　|
|DeserializeToObjectExec|doConsume|　|　|
|SerializeFromObjectExec|doConsume|　|　|
|MapElementsExec|doConsume|　|　|
|SortExec|　||doProduce|
|InputAdapter|　|doProduce|　|
|WholeStageCodegenExec|　|　|nothing|
|HashAggregateExec|　||doProduce|
|DebugExec|doConsume|　|　|
|BroadcastHashJoinExec|doConsume|　|　|
|SortMergeJoinExec|　|doProduce|　|
|BaseLimitExec|
|(Local\Global)|doConsume|　|　|

所以，由上知，支持Codegen的物理计划：
1. 所有Exec的Consume方法调用了自己父节点的doConsume；而Consume是被自身的doProduce或者doConsume调用；
2. WholeStageCodegenExec的Consume没人调用，它的doConsume终结了这种循环调用；
3. RangeExec\RowDataSourceScanExec\BatchedDataSourceScanExec\InputAdapter\SortMergeJoinExec作为produce->doProduce调用的结束点；它其实也是Consume->doConsume的开始点，因为其doProduce调用了Consume,而没有提供doConsume给子节点调用；即这2个过程是链接起来的；
4. SortExec和HashAggregateExec比较特殊；其doProduce调用了Consume，又提供doConsume给子节点调用。从后一小节可知道，其doConsume终结了子链路的Consume->doConsume调用。即这2个物理计划结束子链路的Consume->doConsume调用，又开始新的Consume->doConsume调用，执行点是doProduce方法。

###doConsume
这个方法的定义注释说：生成处理子物理计划行的Java源代码
```
Generate the Java source code to process the rows from child SparkPlan..
```
这个方法在几乎每个Exec的实现都比较丰富，对比produce->doProduce过程的10个透传，在Codegen的代码生成过程中，Consume->doConsume生成了更多的代码。
如上节描述的：
1. 有10个物理计划的doConsume调用了自身的Consume方法，他们是；
2. 有5节点没有实现doConsume方法，其Consume被doProduce调用，所以他们其实是Consume->doConsume的起点(也是produce->doProduce的终点，doProduce是他们的链接点)；
3. WholeStageCodegenExec\SortExec\HashAggregateExec的doConsume没有调用Consume，所以他们是Consume->doConsume的终点，这和上节分析一致；
4. SortExec\HashAggregateExec有点特殊，doConsume其结束了子链的Consume-> doConsume，但又开始了父链的Consume-> doConsume；所以他们将当前Codegen链拆成了2个生产消费链，子链是父链的一部分，最后生成的代码还是父链的发起点WholeStageCodegenExec。

|class|default|define|exception|
|:--:|:--:|:--:|:--:|
|ProjectExec|Consume|　|　|
|FilterExec|Consume|　|　|
|SampleExec|Consume|　|　|
|RangeExec|　|nothing|　|
|RowDataSourceScanExec|　|nothing|　|
|BatchedDataSourceScanExec|　|nothing|　|
|ExpandExec|Consume|　|　|
|DeserializeToObjectExec|Consume|　|　|
|SerializeFromObjectExec|Consume|　|　|
|MapElementsExec|Consume|　|　|
|SortExec|　|　|end|
|InputAdapter|　|nothing|　|
|WholeStageCodegenExec|　|　|end|
|HashAggregateExec|　|　|end|
|DebugExec|Consume|　|　|
|BroadcastHashJoinExec|Consume|　|　|
|SortMergeJoinExec|　|nothing|　|
|BaseLimitExec|
|(Local\Global)|Consume|　|　|

###evaluateRequiredVariables
```
protected def evaluateRequiredVariables(
    attributes: Seq[Attribute],
    variables: Seq[ExprCode],
    required: AttributeSet): String = {
  val evaluateVars = new StringBuilder
  variables.zipWithIndex.foreach { case (ev, i) =>
    if (ev.code != "" && required.contains(attributes(i))) {
      evaluateVars.append(ev.code.trim + "\n")
      ev.code = ""
    }
  }
  evaluateVars.toString()
}
```


其中attributes是成员属性，variables是已经完成计算和代码生成的成员对应变量，required则是当前需要处理的变量。
evaluateRequiredVariables完成的功能是，遍历variables，找到当前ExprCode中和required匹配的变量，提取其code部分返回（同时将该ExprCode的code部分置为空）

###evaluateVariables
Variables为当前已计算表达式，此方法提取code非空表达式的code返回，并将所有code置空。
```
protected def evaluateVariables(variables: Seq[ExprCode]): String = {
  val evaluate = variables.filter(_.code != "").map(_.code.trim).mkString("\n")
  variables.foreach(_.code = "")
  evaluate
}
```
##WholeStageCodegen
- 只有一个构造入参成员：child：SparkPlan
- output\outputPartitioning\outputOrdering都是调用child的相同执行
- 其 doProduce\inputRDDs 都是异常(不能被调用)，Produce也不会调用
-- 从inputRDDs可知，它直接读的child的inputRDDs，其实这里完全可以定义自身的inputRDDs为通用的读child，而doExecute中则引用自己的inputRDDs，而不需要在实现中调child；
-- Produce虽然没有显示表现和其他Exec不同，但其实此节点不会调用。
- 主要是doExecute\doCodeGen\doConsume
###doConsume
###
###doExecute


###doCodeGen
doCodeGen总是先调用child的produce()，其生成一个while读输入写入当前BufferedRowIterator(此while被shouldStop破坏了，后续可以验证下)。
此段代码被child生成，由于child必须继承自CodegenSupport，其其实调用了CodegenSupport的(produce->doProduce)\(Consume->doConsume)体系协助生成代码
doCodeGen主干部分还是生成一个BufferedRowIterator类，包括init()方法，和processNext()方法，其中processNext方法主体就是上节child生成的代码。
注意：上面这个例子中，wholeStage的child是一个普通的文件扫描物理计划，所以是这样的。如果其是一个聚合物理计划，则声称的代码更更复杂一点。其会在processNext的开头判断是否做了初始聚合（只执行一次），初始聚合会将所有input聚合完成，然后每次从完成的iterator里取一条append到Buffer。
所以有聚合和无聚合这里差别很大，需要测试性能差别。
同理，聚合
```
public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }
      
final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
	private Object[] references;
	private scala.collection.Iterator[] inputs;
	private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows;
	private scala.collection.Iterator scan_input;
	private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows1;
	private scala.collection.Iterator scan_input1;
	private org.apache.spark.sql.execution.metric.SQLMetric scan_numOutputRows2;
	private scala.collection.Iterator scan_input2;

	public GeneratedIterator(Object[] references) {
	  this.references = references;
	}

	public void init(int index, scala.collection.Iterator[] inputs) {
		partitionIndex = index;
		this.inputs = inputs;
		this.scan_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
		scan_input = inputs[0];
		this.scan_numOutputRows1 = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
		scan_input1 = inputs[0];
		this.scan_numOutputRows2 = (org.apache.spark.sql.execution.metric.SQLMetric) references[2];
		scan_input2 = inputs[0];	  
	}

	protected void processNext() throws java.io.IOException {
	    while (scan_input.hasNext()) {
		  InternalRow scan_row = (InternalRow) scan_input.next();
		  scan_numOutputRows.add(1);
		  append(scan_row);
		  if (shouldStop()) return;
		}
	}
}
```

##InputAdapter
- 只有一个构造入参成员：child：SparkPlan
- output\outputPartitioning\outputOrdering\doExecute\doExecuteBroadcast都是调用child的相同执行
- inputRDDs 是调用child的doExecute，即和自己的doExecute同样结果
- 主要方法是doProduce
```
/* InputAdapter is used to hide a SparkPlan from a subtree that support codegen.
This is the leaf node of a tree with WholeStageCodegen that is used to generate code that consumes an RDD iterator of InternalRow. */
/* InputAdapter用于隐藏支持代码生成的子树中的物理计划。
它是一个WholeStageCodegen树的叶节点，WholeStageCodegen用于生成消耗InternalRow的RDD迭代器的代码。*/
```

##示例
我们用一个简单的例子实践WholeStageCodegenExec的codegen过程：
```
insert overwrite table id3 select name,id from id2 where id is not null
== Physical Plan ==
InsertIntoHiveTable MetastoreRelation default, id3, true, false
+- *Filter isnotnull(id#11)
   +- HiveTableScan [name#10, id#11], MetastoreRelation default, id2
```
这里例子里，HiveTableScan 和Filter 之间插入了InputAdapter；InsertIntoHiveTable 和Filter 之间插入了WholeStageCodegenExec。

###WholeStageCodegenExec
此处WholeStageCodegenExec的doConsume是代码生成的尾节点：其代码生成结果如下：

对比源代码，看出，WholeStageCodegenExec的doConsume仅仅是完成入参携带代码的整合；即：
1. Row.code携带了行运算的代码；
2. Row.value携带了上述代码中最后直接过的变量名；
3. ctx.copyResult标识了代码最后计算结果是否需要拷贝；
4. WholeStageCodegenExec的doConsume在生成的代码后加上一行append（结果）


###FilterExec
此处WholeStageCodegenExec的doConsume是代码生成的尾节点：其代码生成结果如下：
```
Begin doConsume:org.apache.spark.sql.execution.WholeStageCodegenExec
local: ctx.copyResult = false
local: row.code = 
        boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
        UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
        filter_holder.reset();        
        filter_rowWriter.zeroOutNullBytes();
        filter_rowWriter.write(0, inputadapter_value);
        if (inputadapter_isNull1) {
          filter_rowWriter.setNullAt(1);
        } else {
          filter_rowWriter.write(1, inputadapter_value1);
        }
        filter_result.setTotalSize(filter_holder.totalSize());
local: row.value = filter_result
result:
        boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
        UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
        filter_holder.reset();        
        filter_rowWriter.zeroOutNullBytes();
        filter_rowWriter.write(0, inputadapter_value);
        if (inputadapter_isNull1) {
          filter_rowWriter.setNullAt(1);
        } else {
          filter_rowWriter.write(1, inputadapter_value1);
        }
        filter_result.setTotalSize(filter_holder.totalSize());
        append(filter_result);
End doConsume:========================================================
```
对比源代码，看出，WholeStageCodegenExec的doConsume仅仅是完成入参携带代码的整合；即：
1. Row.code携带了行运算的代码；
2. Row.value携带了上述代码中最后直接过的变量名；
3. ctx.copyResult标识了代码最后计算结果是否需要拷贝；
4. WholeStageCodegenExec的doConsume在生成的代码后加上一行append（结果）
```
override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
  println(s"Begin doConsume:${this.getClass.getName}")
  val numOutput = metricTerm(ctx, "numOutputRows")
  def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
    val bound = BindReferences.bindReference(c, attrs)
    val evaluated = evaluateRequiredVariables(child.output, in, c.references)
    val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
    val nullCheck = if (bound.nullable) {
      s"${ev.isNull} || "
    } else {
      s""
    }
    s"""
       |$evaluated
       |${ev.code}
       |if (${nullCheck}!${ev.value}) continue;
     """.stripMargin
  }
 ctx.currentVars = input
 val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
  val generated = otherPreds.map { c =>
    val nullChecks = c.references.map { r =>
      val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
      if (idx != -1 && !generatedIsNotNullChecks(idx)) {
        generatedIsNotNullChecks(idx) = true
        genPredicate(notNullPreds(idx), input, child.output)
      } else {
        ""
      }
    }.mkString("\n").trim
    s"""
       |$nullChecks
       |${genPredicate(c, input, output)}
     """.stripMargin.trim
  }.mkString("\n")
  val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
    if (!generatedIsNotNullChecks(idx)) {
      genPredicate(c, input, child.output)
    } else {
      ""
    }
  }.mkString("\n")
   val resultVars = input.zipWithIndex.map { case (ev, i) =>
    if (notNullAttributes.contains(child.output(i).exprId)) {
      ev.isNull = "false"
    }
    ev
  }
  val filterTimeCodegen = metricTerm(ctx, "filterTimeCodegen")
  val beforeFilter = ctx.freshName("beforeFilter")
  val result = s"""
     |long $beforeFilter = System.nanoTime();
     |$generated
     |$nullChecks
     |$numOutput.add(1);
     |$filterTimeCodegen.add((System.nanoTime() - $beforeFilter));
     |${consume(ctx, resultVars)}
   """.stripMargin
  println(s"${result}\n" +
    s"End doConsume:========================================================")
  result
}

```
