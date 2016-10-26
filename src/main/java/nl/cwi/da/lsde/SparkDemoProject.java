package nl.cwi.da.lsde;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.opensky.libadsb.Decoder;
import org.opensky.libadsb.tools;
import org.opensky.libadsb.msgs.ModeSReply;

public class SparkDemoProject {

  private static StructType retTpe = DataTypes.createStructType(
      Arrays.asList(new StructField("icao", DataTypes.StringType, false, null),
          new StructField("type", DataTypes.StringType, false, null)));

  private static UDF2<String, Double, Row> adsb_decode = new UDF2<String, Double, Row>() {
    private static final long serialVersionUID = 1L;

    public Row call(String raw, Double timestamp) throws Exception {
      ModeSReply msg;

      try {
        msg = Decoder.genericDecoder(raw);
      } catch (Exception e) {
        return null;
      }

      return RowFactory.create(tools.toHexString(msg.getIcao24()),
          msg.getType().toString());
    }
  };

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      return;
    }
    SparkConf conf = new SparkConf();
    // conf.setMaster("local").setAppName("fuu")); // required for in-eclipse
    // runs
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlCtx = new SQLContext(sc);
    sqlCtx.udf().register("adsb_decode", adsb_decode, retTpe);

    DataFrame df = sqlCtx.read().format("com.databricks.spark.avro")
        .load(args[0]).limit(100)
        .select(col("timestamp"),
            callUDF("adsb_decode", col("rawMessage"), col("timestamp"))
                .as("decoded"))
        .filter("decoded is not null")
        .select(col("timestamp"), col("decoded.*"));

    df.collect();
    df.show();
    df.printSchema();

  }

}
