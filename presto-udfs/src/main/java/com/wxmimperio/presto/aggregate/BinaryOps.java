package com.wxmimperio.presto.aggregate;

@AggregationFunction("binary_or")
@Description("This function will turn all character except 0 to 1.")
public class BinaryOps {

    @InputFunction
    public static void input(SliceState state, @SqlType(StandardTypes.VARCHAR) Slice input) {
        String intStateStr = input.toStringUtf8();
        state.setSlice(Slices.utf8Slice(getBinaryOr(null == state.getSlice() ? null : state.getSlice().toStringUtf8(), intStateStr)));
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState inState) {
        String intStateStr = inState.getSlice().toStringUtf8();
        state.setSlice(Slices.utf8Slice(getBinaryOr(null == state.getSlice() ? null : state.getSlice().toStringUtf8(), intStateStr)));
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(SliceState state, BlockBuilder out) {
        if (state.getSlice() == null) {
            out.appendNull();
        } else {
            VarcharType.VARCHAR.writeString(out, state.getSlice().toStringUtf8());
        }
    }

    private static String getBinaryOr(String str1, String str2) {
        if (null != str1) str1 = str1.replaceAll("[^0]", "1");
        if (null != str2) str2 = str2.replaceAll("[^0]", "1");
        if (null == str1) return str2;
        if (null == str2) return str1;
        int length = str1.length() > str2.length() ? str1.length() : str2.length();
        if (str1.length() < length) str1 = StringUtils.repeat("0", length - str1.length()) + str1;
        if (str2.length() < length) str2 = StringUtils.repeat("0", length - str2.length()) + str2;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            sb.append((char) (str1.charAt(i) | str2.charAt(i)));
        }
        return sb.toString();
    }

}
