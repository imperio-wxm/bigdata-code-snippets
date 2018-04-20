package com.wxmimperio.presto;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.wxmimperio.presto.aggregate.BinaryOps;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Created by wxmimperio on 2018/2/19.
 */
public class TestFunctionsPlugin implements Plugin {

    /*private TypeManager typeManager;

    @Inject
    public void setTypeManager(TypeManager typeManager) {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }
*/
    @Override
    public <T> List<T> getServices(Class<T> type) {
        if (type == FunctionFactory.class) {
            //return ImmutableList.of(type.cast(new TestFunctionFactory(typeManager)));
            final FunctionListBuilder builder = new FunctionListBuilder();
            builder.scalars(TestUdf.class)
                    .scalars(Ip2Long.class)
                    .scalars(TimestampToDate.class)
                    .scalars(DateSub.class)
                    .scalars(DateOpts.class)
                    .aggregate(BinaryOps.class);
            FunctionFactory factory = () -> builder.getFunctions();
            return ImmutableList.of(type.cast(factory));
        } else {
            return ImmutableList.of();
        }
    }
}
