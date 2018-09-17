package com.att.org.weather.WeatherStationDemo;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;

import com.att.org.weather.WeatherStationDemo.model.WeatherDetails;

import scala.Function1;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WeatherStationController 
{

	public static void main(String[] args) throws IOException
	{

		 //Create Java spark context
		 SparkConf sparkConf = new SparkConf().setAppName("Weather data Demo");
		 @SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		//Create Spark Context
		 SparkContext sc = jsc.sc();
		 SQLContext sqlContext = new SQLContext(sc);
		//SQLContext sqlContext =  createSqlContext();
		 DataFrame df =  sqlContext.read().text("/user/ndulam/weather_raw/");
		 JavaRDD<Row> withFileNameJavaRDD =  df.select(org.apache.spark.sql.functions.input_file_name().alias("filename"),df.col("value")).javaRDD();
/*
		 JavaRDD<WeatherDetails> weatherJavaRDD =  withFileNameJavaRDD.map(new Function<Row, WeatherDetails>()
		 {
	
			private static final long serialVersionUID = 1L;

			public WeatherDetails call(Row row)
			  {
				  String stationNumber =  row.getString(0).split(":")[2].split("/")[4].split("-")[0];
				  String tmp =  row.getString(1);
				  String year = tmp.split("\\s+")[0];
				  String month =  tmp.split("\\s+")[1];
				  String day =  tmp.split("\\s+")[2];
				  String hour = tmp.split("\\s+")[3];
				  String air_temperature = tmp.split("\\s+")[4];
				  String dew_temperature = tmp.split("\\s+")[5];
				  String sea_level_pressure = tmp.split("\\s+")[6];
				  String wind_direction = tmp.split("\\s+")[7];
				  String wind_speed_rate = tmp.split("\\s+")[8];
				  String sky_condition = tmp.split("\\s+")[9];
				  String liqd_per_dept_dimen_hr = tmp.split("\\s+")[10];
				  String liqd_per_dept_dimen_6hr = tmp.split("\\s+")[11];
				  return new WeatherDetails(stationNumber,year,month,day,hour,air_temperature,dew_temperature,sea_level_pressure,wind_direction,wind_speed_rate,sky_condition,liqd_per_dept_dimen_hr,liqd_per_dept_dimen_6hr);
			  }
		 });
		 
		 //filter missing data values
		 JavaRDD<WeatherDetails> filterMissingData = weatherJavaRDD.filter(new Function<WeatherDetails, Boolean>() 
		 {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(WeatherDetails wd) throws Exception 
			{
				if(wd.getDew_temperature()==-9999 || wd.getAir_temperature()==-9999 || wd.getSea_level_pressure()==-9999 
						|| wd.getWind_direction()==-9999 || wd.getWind_speed_rate()==-9999 || wd.getSky_condition()==-9999 
						|| wd.getLiqd_per_dept_dimen_hr()==-9999 || wd.getLiqd_per_dept_dimen_6hr()==-9999)
					return false;
				else	
					return true;
			}
		});
		 
		 //Create data Frame
		 DataFrame weatherDF =  sqlContext.createDataFrame(filterMissingData, WeatherDetails.class);
		 
		 String pattern = "dd-MM-yyyy";
		 SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		 String date = simpleDateFormat.format(new Date());
		 		 
		 //repartition to create 10 files and write as parquet file
		 weatherDF.repartition(10) .write().mode(SaveMode.Overwrite).parquet("/user/ndulam/weather_refined_parq/as_of_date="+date);
		*/
		 DataFrame weatheTableDF =  sqlContext.parquetFile("/user/ndulam/weather_refined_parq");
		 
		 weatheTableDF.persist(StorageLevel.MEMORY_AND_DISK());
		 
		 //each station wise, hour wise average temperature
		 DataFrame stnAvgTempHourWise = weatheTableDF.groupBy("stationNumber","hour").agg(functions.avg(weatheTableDF.col("air_temperature")).alias("avg_temperature"));
		 stnAvgTempHourWise.repartition(10).write().mode(SaveMode.Overwrite).parquet("cstnAvgTempHourWise");
         
		 //Each station wise record count 
		DataFrame stationWiseCount =  weatheTableDF.groupBy("stationNumber").agg(functions.count(weatheTableDF.col("air_temperature")).alias("Total_Records"));
		stationWiseCount.write().mode(SaveMode.Overwrite).parquet("/user/ndulam/stnWiseCount");
		
		
		Map<Integer,String> skyConditionMap  = broadCastSkyConditionMap();
		
		DataFrame pivotedDataFrame = weatheTableDF.groupBy("stationNumber").pivot("sky_condition").agg(functions.sum("air_temperature").alias("sum_temp"));
		pivotedDataFrame.write().mode(SaveMode.Overwrite).parquet("/user/ndulam/pivotedData");
		
		// average wind speed rate each station month wise
		DataFrame avgwindSpdRatestnmnthWise = weatheTableDF.groupBy("stationNumber","month").agg(functions.avg("wind_speed_rate").alias("sum_wid_spd"));
		avgwindSpdRatestnmnthWise.write().mode(SaveMode.Overwrite).parquet("/user/ndulam/avgwindSpdRatestnmnthWise");
		
		sqlContext.udf().register("skyCondtnMap", new UDF1<Integer, String>() {
		      @Override
		      public String call(Integer val) {
		        
		    	  if(val==0)
		    		  return "None";
		    	  else if(val==1)
		    		  return "One_okta";
		    	  else if(val==2)
		    		  return "Two_okta";
		    	  else if(val==3)
		    		  return "three_okta";
		    	  else if(val==4)
		    		  return "foru_okta";
		    	  else if(val==5)
		    		  return "five_okta";
		    	  else if(val==6)
		    		  return "six_okta";
		    	  else if(val==7)
		    		  return "seven_okta";
		    	  else if(val==8)
		    		  return "eight_okta";
		    	  else if(val==9)
		    		  return "Sky_obscured";
		    	  else if(val==10)
		    		  return "Partial_obscuration";
		    	  else if(val==11)
		    		  return "Thin_scattered";
		    	  else if(val==12)
		    		  return "Scattered";
		    	  else if(val==13)
		    		  return "Dark_scattered";
		    	  else if(val==14)
		    		  return "Thin_broken";
		    	  else if(val==15)
		    		  return "Broken";
		    	  else if(val==16)
		    		  return "Dark_broken";
		    	  else if(val==17)
		    		  return "Thin_overcast";
		    	  else if(val==18)
		    		  return "Overcast";
		    	  else if(val==19)
		    		  return "Dark_overcast";
		    	  else 
		    		  return "Not_valid";
		      }
		    }, DataTypes.StringType);
		
		
		//Register weather data frame as temp table
		weatheTableDF.registerTempTable("WeatherTable");
		System.out.println("Printing Temp Table Schema");
		
		weatheTableDF.printSchema();
		sqlContext.sql("describe WeatherTable").collect();
		DataFrame tempDF = sqlContext.sql("select stationNumber,year,month,day,hour,air_temperature,dew_temperature,sea_level_pressure,wind_direction,wind_speed_rate,"
				+ "skyCondtnMap(sky_condition) as skyCondtn,liqd_per_dept_dimen_hr,liqd_per_dept_dimen_6hr from WeatherTable");
		
		DataFrame skycndtnMap = tempDF.groupBy("stationNumber","year").pivot("skyCondtn").agg(functions.avg("sea_level_pressure").alias("sum_sea_lvl_presur"));
		skycndtnMap.write().mode(SaveMode.Overwrite).parquet("/user/ndulam/skycndtnMap");
	}
	
	public static class skyConditionLkp implements UDF1<Integer,String> {
	    @Override
	    public String call(Integer val) {
	    	if(val==0)
	    		  return "None";
	    	  else if(val==1)
	    		  return "One okta";
	    	  else if(val==2)
	    		  return "Two okta";
	    	  else if(val==3)
	    		  return "three okta";
	    	  else if(val==4)
	    		  return "foru okta";
	    	  else if(val==5)
	    		  return "five okta";
	    	  else if(val==6)
	    		  return "six okta";
	    	  else if(val==7)
	    		  return "seven okta";
	    	  else if(val==8)
	    		  return "eight okta";
	    	  else if(val==9)
	    		  return "Sky obscured";
	    	  else if(val==10)
	    		  return "Partial obscuration";
	    	  else if(val==11)
	    		  return "Thin scattered";
	    	  else if(val==12)
	    		  return "Scattered";
	    	  else if(val==13)
	    		  return "Dark scattered";
	    	  else if(val==14)
	    		  return "Thin broken";
	    	  else if(val==15)
	    		  return "Broken";
	    	  else if(val==16)
	    		  return "Dark broken";
	    	  else if(val==17)
	    		  return "Thin overcast";
	    	  else if(val==18)
	    		  return "Overcast";
	    	  else if(val==19)
	    		  return "Dark overcast";
	    	  else 
	    		  return "Not_valid";
	    }
	  }
	
	private static Map<Integer,String> broadCastSkyConditionMap() 
	{
		Map<Integer,String> skyConditionMap = new HashMap<Integer, String>();
		 skyConditionMap.put(0,"None");
		 skyConditionMap.put(1,"One okta");
		 skyConditionMap.put(2,"Two oktas");
		 skyConditionMap.put(3,"Three oktas");
		 skyConditionMap.put(4,"Four oktas");
		 skyConditionMap.put(5,"Five oktas");
		 skyConditionMap.put(6,"Six oktas");
		 skyConditionMap.put(7,"Seven oktas");
		 skyConditionMap.put(8,"Eight oktas");
		 skyConditionMap.put(9,"Sky obscured");
		skyConditionMap.put(10,"Partial obscuration");
		skyConditionMap.put(11,"Thin scattered");
		skyConditionMap.put(12,"Scattered");
		skyConditionMap.put(13,"Dark scattered");
		skyConditionMap.put(14,"Thin broken");
		skyConditionMap.put(15,"Broken");
		skyConditionMap.put(16,"Dark broken");
		skyConditionMap.put(17,"Thin overcast");
		skyConditionMap.put(18,"Overcast");
		skyConditionMap.put(19,"Dark overcast");
		
		return skyConditionMap;
			
	}

	
	
	private static SQLContext createSqlContext()
	{
		 //Create Java spark context
		 SparkConf sparkConf = new SparkConf().setAppName("Weather data Demo");
		 @SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		//Create Spark Context
		 SparkContext sc = jsc.sc();
		 SQLContext sqlContext = new SQLContext(sc);
		return sqlContext;
	}


	private static StructType getISDStructType()
	{
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("month", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("day", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("hour", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("air_temperature", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("dew_temperature", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("sea_level_pressure", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("wind_direction", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("wind_speed_rate", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("sky_condition", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("liqd_per_dept_dimen_hr", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("liqd_per_dept_dimen_6hr", DataTypes.IntegerType, true));
		StructType struct = DataTypes.createStructType(fields);				
		return struct;		
	}
	
	//Row jobRow = RowFactory.create("","");
	//List<Row> RowList= new ArrayList<Row>();
	//RowList.add(jobRow);	
	//JavaRDD<Row> rowRDD = sc.parallelize(RowList);
	//Dataset<Row> jobDS = sparkSession.sqlContext().createDataFrame(rowRDD, getISDStructType()).toDF();	
		
}
