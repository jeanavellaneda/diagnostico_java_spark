package diagnostico_java_spark;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import minsait.ttaa.datio.engine.Transformer;

class test {

	static SparkSession spark = SparkSession.builder()
			.master(SPARK_MODE)
			.getOrCreate();

	@Test
	void testFilterPlayerCatValueAorB() {
		Transformer engine = new Transformer(spark);
		Dataset<Row> row = engine.getRetorno();
		boolean value = false;

		for (Row rowValue : row.collectAsList()) {
			String valueColumn = rowValue.getAs("player_cat").toString();
			if (valueColumn.contains("A") || valueColumn.contains("B"))
				value = true;
		}
		Assert.assertTrue(value);
	}

	@Test
	void testFilterPlayerCatValueCandPotentialVsOverallThanOnePointFifteen() {
		Transformer engine = new Transformer(spark);
		Dataset<Row> row = engine.getRetorno();
		boolean value = false;

		for (Row rowValue : row.collectAsList()) {
			String valueColumn = rowValue.getAs("player_cat").toString();
			if (valueColumn.contains("C")
					&& (Double.parseDouble(rowValue.getAs("potential_vs_overall").toString()) > 1.15))
				value = true;
		}
		Assert.assertTrue(value);
	}

	@Test
	void testFilterPlayerCatValueDandPotentialVsOverallThanOnePointTwentyFive() {
		Transformer engine = new Transformer(spark);
		Dataset<Row> row = engine.getRetorno();
		boolean value = false;

		for (Row rowValue : row.collectAsList()) {
			String valueColumn = rowValue.getAs("player_cat").toString();
			if (valueColumn.contains("D")
					&& (Double.parseDouble(rowValue.getAs("potential_vs_overall").toString()) > 1.25))
				value = true;
		}
		Assert.assertTrue(value);
	}
}