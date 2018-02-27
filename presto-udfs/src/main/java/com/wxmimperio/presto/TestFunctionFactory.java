package com.wxmimperio.presto;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

/**
 * Created by wxmimperio on 2018/2/28.
 */
public class TestFunctionFactory implements FunctionFactory {

    private final TypeManager typeManager;

    public TestFunctionFactory(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    @Override
    public List<SqlFunction> listFunctions() {
        return new FunctionListBuilder()
                .scalar(TestUdf.class)
                .getFunctions();
    }
}
