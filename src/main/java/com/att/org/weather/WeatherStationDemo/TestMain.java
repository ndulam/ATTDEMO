package com.att.org.weather.WeatherStationDemo;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestMain {

	public static void main(String[] args) 
	{
		String tmp = "hdfs://quickstart.cloudera:8020/user/ndulam/weather_raw/010010-99999-2016";
		
		String t = tmp.split(":")[2].split("/")[4].split("-")[0];
		System.out.println(t);
		 String pattern = "dd-MM-yyyy";
		 SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		 String date = simpleDateFormat.format(new Date());
		 System.out.println(date);
	}

}
