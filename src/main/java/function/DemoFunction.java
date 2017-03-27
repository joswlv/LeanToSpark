package function;

import model.DemoInfoKey;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * spark_test - 2017. 3. 27..
 */
public class DemoFunction {

    private static final Tuple2<DemoInfoKey, Integer> EMPTY_TUPLE2 = new Tuple2<DemoInfoKey, Integer>(null, null);
    public static final DemoInfoKey EMPTY_DEMOINFOKEY = new DemoInfoKey(null,null,null,null);
    /*
     input 형태 :
     0000040B-D236-4AF8-B95A-80B5FBA306A1    0       2       0
     0000040F-91BF-4726-A3E9-B53836D3848D    1       6       2
     0000067C-8719-4E41-928C-417965803D61    1       9       1
     */
    public static final PairFunction refineLogParserPairFunction = new PairFunction<String, DemoInfoKey, Integer>() {
        public Tuple2<DemoInfoKey, Integer> call(String s) throws Exception {
            if (s.isEmpty()) return DemoFunction.EMPTY_TUPLE2;
            String[] temp = s.split("\\t");
            if(temp.length < 4) return DemoFunction.EMPTY_TUPLE2;

            return new Tuple2<DemoInfoKey, Integer>(new DemoInfoKey(temp[0],temp[1],temp[2],temp[3]),1);
        }
    };

    public static final Function2 demoCount = new Function2<Integer,Integer,Integer>() {
        public Integer call(Integer o, Integer o2) throws Exception {
            return o + o2;
        }
    };

    public static final Function demoFilter = new Function<Tuple2<DemoInfoKey,Integer>, Boolean>() {
        public Boolean call(Tuple2<DemoInfoKey, Integer> tuple2) throws Exception {
            if (tuple2._2() == 1) return true;
            else return false;
        }
    };

    public static final Function refinePrintFunction = new Function<Tuple2<DemoInfoKey,Integer>, String>() {
        public String call(Tuple2<DemoInfoKey, Integer> tuple) throws Exception {
            return String.format("%s\t%s\t%s\t%s",tuple._1().getUid(),tuple._1().getAge_code(),tuple._1().getGender_code(),tuple._1().getMarriage());
        }
    };

    public static final Function refineLogParserFunction = new Function<String, DemoInfoKey>() {
        public DemoInfoKey call(String s) throws Exception {
            if (s.isEmpty()) return EMPTY_DEMOINFOKEY;
            String[] temp = s.split("\t");
            if(temp.length < 4) return EMPTY_DEMOINFOKEY;
            return new DemoInfoKey(temp[0],temp[1],temp[2],temp[3]);
        }
    };

    public static PairFunction getRefineLogParserPairFunction(){ return refineLogParserPairFunction; }
    public static Function2 getDemoCount(){ return demoCount; }
    public static Function getDemoFilter(){ return demoFilter; }

    public static Function getRefineLogParserFunction() { return refineLogParserFunction; }
    public static Function getRefinePrintFunction() { return refinePrintFunction; }
}

