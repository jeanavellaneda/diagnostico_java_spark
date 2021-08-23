package minsait.ttaa.datio.engine;

import static minsait.ttaa.datio.common.Common.HEADER;
import static minsait.ttaa.datio.common.Common.INFER_SCHEMA;
import static minsait.ttaa.datio.common.Common.INPUT_PATH;
import static minsait.ttaa.datio.common.naming.PlayerInput.age;
import static minsait.ttaa.datio.common.naming.PlayerInput.clubName;
import static minsait.ttaa.datio.common.naming.PlayerInput.heightCm;
import static minsait.ttaa.datio.common.naming.PlayerInput.longName;
import static minsait.ttaa.datio.common.naming.PlayerInput.nationality;
import static minsait.ttaa.datio.common.naming.PlayerInput.overall;
import static minsait.ttaa.datio.common.naming.PlayerInput.potential;
import static minsait.ttaa.datio.common.naming.PlayerInput.shortName;
import static minsait.ttaa.datio.common.naming.PlayerInput.teamPosition;
import static minsait.ttaa.datio.common.naming.PlayerInput.weightKg;
import static minsait.ttaa.datio.common.naming.PlayerOutput.playerCat;
import static minsait.ttaa.datio.common.naming.PlayerOutput.potentialVsOverall;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import minsait.ttaa.datio.common.Common;
import minsait.ttaa.datio.util.PropertyValues;

public class Transformer extends Writer {
	private SparkSession spark;
	PropertyValues properties = new PropertyValues();
	Dataset<Row> df;

	public Transformer(@NotNull SparkSession spark) {
		this.spark = spark;
		//Dataset<Row> df = readInput();
		df = readInput();
		
		df.printSchema();

		df = cleanData(df);
		df = exampleWindowFunction(df);
		
		df = columnSelection(df);
		// for show 100 records after your transformations and show the Dataset schema
		df.show(200, false);
		df.printSchema();
		getRetorno();

		// Uncomment when you want write your final output
		// write(df);
	}

	public Dataset<Row> getRetorno() {
		return df;
	}

	/**
	 * Item #1
	 * @param df
	 * @return
	 */
	private Dataset<Row> columnSelection(Dataset<Row> df) {
		return df.select(shortName.column(), 
				longName.column(), 
				age.column(), 
				heightCm.column(), 
				weightKg.column(),
				clubName.column(), 
				potential.column(), 
				overall.column(), 
				nationality.column(), 
				teamPosition.column(),
				playerCat.column(), 
				potentialVsOverall.column());
	}

	/**
	 * @return a Dataset readed from csv file
	 */
	private Dataset<Row> readInput() {
		Dataset<Row> df = spark.read()
				.option(HEADER, true)
				.option(INFER_SCHEMA, true)
				.csv(INPUT_PATH);
		return df;
	}

	/**
	 * @param df
	 * @return a Dataset with filter transformation applied column team_position !=
	 *         null && column short_name != null && column overall != null
	 */
	private Dataset<Row> cleanData(Dataset<Row> df) {
		df = df.filter(teamPosition.column().isNotNull()
				.and(shortName.column().isNotNull())
				.and(overall.column().isNotNull()));

		return df;
	}

	/**
	 * @param df is a Dataset with players information (must have team_position and
	 *           height_cm columns)
	 * @return add to the Dataset the column "cat_height_by_position" by each
	 *         position value cat A for if is in 20 players tallest cat B for if is
	 *         in 50 players tallest cat C for the rest
	 */
	private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
		WindowSpec w = Window.partitionBy(nationality.column())
				.partitionBy(teamPosition.column())
				.orderBy(overall.column().desc());

		Column rank = rank().over(w);

		Column rulePlayerCat = categorizePlayer(rank);
		Column rulePotentialOverall = versusPotentialOverall();
		Column ruleFilterPlayerCatPotOverall = ruleFilterPlayerCatPotOverall(col("player_cat"),
				col("potential_vs_overall"));

		df = df.withColumn(playerCat.getName(), rulePlayerCat)
			   .withColumn(potentialVsOverall.getName(), rulePotentialOverall)
			   .filter(ruleFilterPlayerCatPotOverall)
			   .filter(filterForAge());
		return df;
	}
	
	/**
	 * Item #2
	 * @param rank column the windows
	 * @return filter and categorize (A,B,C,D)
	 */
	private Column categorizePlayer(Column rank) {
		return when(rank.leq(3), Common.CATEGORY_A)
				.when(rank.leq(5), Common.CATEGORY_B)
				.when(rank.leq(10), Common.CATEGORY_C)
				.otherwise(Common.CATEGORY_D);
	}

    /**
     * Item #3
     * @return relation between potential and overall
     */
	private Column versusPotentialOverall() {
		return potential.column().divide(overall.column());
	}
	
	/**
	 * Item #4
	 * @param playerCat
	 * @param potentialOverall
	 * @return
	 */
	private Column ruleFilterPlayerCatPotOverall(Column playerCat, Column potentialOverall) {
		return playerCat.equalTo('A').or(playerCat.equalTo('B')
				.or(playerCat.equalTo('C').and(potentialOverall.$greater(1.15)))
				.or(playerCat.equalTo('D').and(potentialOverall.$greater(1.25))));
	}

	/**
	 * Item #5
	 * @return filtered column age
	 */
	private Column filterForAge() {
		if (getValueParam() == 1)
			return age.column().$less(23);
		else
			return age.column().$greater(0);
	}

	/**
	 * @return value 1 or 0, value get the value of file src/main/resources/params
	 */
	private int getValueParam() {
		return Integer.parseInt(properties.getProperty("value"));	
	}
}