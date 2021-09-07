package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();
        Dataset<Row> df1 = readInput();
        Dataset<Row> df2 = readInput();
        Dataset<Row> df3 = readInput();
        Dataset<Row> df4 = readInput(); 
        Dataset<Row> df5 = readInput(); 
       //exercise 1
        df=traningOneColumnSelection(df);
        df.show();
        
       //exercise 2 
        df=generateAgeRangeColum(df);
        df=traningTwoColumnSelection(df);
        df.show(20, false);
        
       //exercise 3
        df=generateColumnAndRule(df);
        df=traningThreeColumnSelection(df);
        df.show(20, false);
        df5=df;
       //exercise 4
        df=generateColumnPotentialVsOverall(df);
        traningFourColumnSelection(df);
        df.show(20, false);
        
        //exercise 5
        df1=filtterAgeRangeAndRankPointOne(df);
        traningFiveColumnSelection(df1);
        df1.show(20, false);
        df2=filtterAgeRangeAndRankPointTwo(df);
        traningFiveColumnSelection(df2);
        df2.show(20, false);
        df3=filtterAgeRangeAndRankPointThree(df);
        traningFiveColumnSelection(df3);
        df3.show(20, false);
        df4=filtterAgeRangeAndRankPointFour(df);
        traningFiveColumnSelection(df4);
        df4.show(20, false);
        //exercise 6
        writeNationality(df5);
        df.printSchema();

        df = cleanData(df);
        df = exampleWindowFunction(df);
        df = columnSelection(df);
        // for show 100 records after your transformations and show the Dataset schema
        df.show(10, false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }
    //traning  1
    //La tabla de salida debe contener las siguientes columnas: short_name, long_name, age, height_cm, weight_kg, nationality, club_name, overall, potential, team_position
    private Dataset<Row> traningOneColumnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column()
                
        );
    }
     private Dataset<Row> traningTwoColumnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column()

        );
    }
     private Dataset<Row> traningThreeColumnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column(),
                rankByNationalityPosition.column()

        );
    }
          private Dataset<Row> traningFourColumnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column(),
                rankByNationalityPosition.column(),
                potentialVsOverall.column()

        );
    } 
       private Dataset<Row> traningFiveColumnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column(),
                rankByNationalityPosition.column(),
                potentialVsOverall.column()

        );
    } 
    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                overall.column(),
                heightCm.column(),
                teamPosition.column(),
                catHeightByPosition.column()
        );
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
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }
      private Dataset<Row> generateAgeRangeColum(Dataset<Row> df) {
        Column rule = when(age.column().$less(24), "A")
                .when(age.column().$less(28), "B")
                .when(age.column().$less(33), "C")
                .when(age.column().$greater(32), "D");
 
        df = df.withColumn(ageRange.getName(), rule);

        return df;
    }
    private Dataset<Row> generateColumnAndRule(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(),teamPosition.column())
                .orderBy(overall.column().desc());

            Column rule =  row_number().over(w);
            
        df = df.withColumn(rankByNationalityPosition.getName(), rule);
        
        return df;
    }

    private Dataset<Row> generateColumnPotentialVsOverall(Dataset<Row> df) {
      df = df.withColumn(potentialVsOverall.getName(),(potential.column().$div(overall.column())));
      
      return df;
    }

    private Dataset<Row> filtterAgeRangeAndRankPointOne(Dataset<Row> df) {
          df = df.filter(
                rankByNationalityPosition.column().$less(4));
        return df;
    }

    private Dataset<Row> filtterAgeRangeAndRankPointTwo(Dataset<Row> df) {
         df = df.filter(
                ageRange.column().$eq$eq$eq("B").or(ageRange.column().$eq$eq$eq("C")).
                        and(potentialVsOverall.column().$greater(1.15)));
        return df;
    }

    private Dataset<Row> filtterAgeRangeAndRankPointThree(Dataset<Row> df) {
        df = df.filter(
                ageRange.column().$eq$eq$eq("A").
                        and(potentialVsOverall.column().$greater(1.25)));
        return df;
    }

    private Dataset<Row> filtterAgeRangeAndRankPointFour(Dataset<Row> df) {
        df = df.filter(
                ageRange.column().$eq$eq$eq("D").
                        and(potentialVsOverall.column().$less(5)));
        return df;
    }




}
