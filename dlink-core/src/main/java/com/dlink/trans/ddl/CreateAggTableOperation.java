package com.dlink.trans.ddl;

import com.dlink.executor.custom.CustomTableEnvironmentImpl;
import com.dlink.trans.AbstractOperation;
import com.dlink.trans.Operation;
import org.apache.flink.table.api.Table;

import java.util.List;

/**
 * CreateAggTableOperation
 *
 * @author wenmo
 * @since 2021/6/13 19:24
 */
public class CreateAggTableOperation extends AbstractOperation implements Operation{

    private String KEY_WORD = "CREATE AGGTABLE";

    public CreateAggTableOperation() {
    }

    public CreateAggTableOperation(String statement) {
        super(statement);
    }

    @Override
    public String getHandle() {
        return KEY_WORD;
    }

    @Override
    public Operation create(String statement) {
        return new CreateAggTableOperation(statement);
    }

    @Override
    public void build(CustomTableEnvironmentImpl stEnvironment) {
        AggTable aggTable = AggTable.build(statement);
        // TODO 根据自定义的AggTable语法执行类似这里的逻辑：
        //  https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/functions/udfs/#table-aggregate-functions
        Table source = stEnvironment.sqlQuery("select * from "+ aggTable.getTable());
        List<String> wheres = aggTable.getWheres();
        if(wheres!=null&&wheres.size()>0) {
            for (String s : wheres) {
                source = source.filter(s);
            }
        }
        Table sink = source.groupBy(aggTable.getGroupBy())
                .flatAggregate(aggTable.getAggBy())
                .select(aggTable.getColumns());
        // TODO 将create aggTable创建的表注册成当前catalog的表
        stEnvironment.registerTable(aggTable.getName(), sink);
    }
}
