package com.att.org.weather.WeatherStationDemo.model;

import java.io.Serializable;

public class WeatherDetails implements Serializable
{
	private int stationNumber;
	private int year;
	private int month;
	private int day;
	private int hour;
	private int air_temperature;
	private int dew_temperature;
	private int sea_level_pressure;
	private int wind_direction;
	private int wind_speed_rate;
	private int sky_condition;
	private int liqd_per_dept_dimen_hr;
	private int liqd_per_dept_dimen_6hr;
	
	
	public WeatherDetails(String stationNumber,String year, String month, String day, String hour, String air_temperature, String dew_temperature,
			String sea_level_pressure, String wind_direction, String wind_speed_rate, String sky_condition,
			String liqd_per_dept_dimen_hr, String liqd_per_dept_dimen_6hr) 
	{
		this.stationNumber = Integer.parseInt(stationNumber);
		this.year = Integer.parseInt(year);
		this.month = Integer.parseInt(month);
		this.day = Integer.parseInt(day);
		this.hour = Integer.parseInt(hour);
		this.air_temperature = Integer.parseInt(air_temperature);
		this.dew_temperature = Integer.parseInt(dew_temperature);
		this.sea_level_pressure = Integer.parseInt(sea_level_pressure);
		this.wind_direction = Integer.parseInt(wind_direction);
		this.wind_speed_rate = Integer.parseInt(wind_speed_rate);
		this.sky_condition = Integer.parseInt(sky_condition);
		this.liqd_per_dept_dimen_hr = Integer.parseInt(liqd_per_dept_dimen_hr);
		this.liqd_per_dept_dimen_6hr = Integer.parseInt(liqd_per_dept_dimen_6hr);
	}
	
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public int getDay() {
		return day;
	}
	public void setDay(int day) {
		this.day = day;
	}
	public int getHour() {
		return hour;
	}
	public void setHour(int hour) {
		this.hour = hour;
	}
	public int getAir_temperature() {
		return air_temperature;
	}
	public void setAir_temperature(int air_temperature) {
		this.air_temperature = air_temperature;
	}
	public int getDew_temperature() {
		return dew_temperature;
	}
	public void setDew_temperature(int dew_temperature) {
		this.dew_temperature = dew_temperature;
	}
	public int getSea_level_pressure() {
		return sea_level_pressure;
	}
	public void setSea_level_pressure(int sea_level_pressure) {
		this.sea_level_pressure = sea_level_pressure;
	}
	public int getWind_direction() {
		return wind_direction;
	}
	public void setWind_direction(int wind_direction) {
		this.wind_direction = wind_direction;
	}
	public int getWind_speed_rate() {
		return wind_speed_rate;
	}
	public void setWind_speed_rate(int wind_speed_rate) {
		this.wind_speed_rate = wind_speed_rate;
	}
	public int getSky_condition() {
		return sky_condition;
	}
	public void setSky_condition(int sky_condition) {
		this.sky_condition = sky_condition;
	}
	public int getLiqd_per_dept_dimen_hr() {
		return liqd_per_dept_dimen_hr;
	}
	public void setLiqd_per_dept_dimen_hr(int liqd_per_dept_dimen_hr) {
		this.liqd_per_dept_dimen_hr = liqd_per_dept_dimen_hr;
	}
	public int getLiqd_per_dept_dimen_6hr() {
		return liqd_per_dept_dimen_6hr;
	}
	public void setLiqd_per_dept_dimen_6hr(int liqd_per_dept_dimen_6hr) {
		this.liqd_per_dept_dimen_6hr = liqd_per_dept_dimen_6hr;
	}

	public int getStationNumber() {
		return stationNumber;
	}

	public void setStationNumber(int stationNumber) {
		this.stationNumber = stationNumber;
	}

	@Override
	public String toString() {
		return "WeatherDetails [stationNumber=" + stationNumber + ", year=" + year + ", month=" + month + ", day=" + day
				+ ", hour=" + hour + ", air_temperature=" + air_temperature + ", dew_temperature=" + dew_temperature
				+ ", sea_level_pressure=" + sea_level_pressure + ", wind_direction=" + wind_direction
				+ ", wind_speed_rate=" + wind_speed_rate + ", sky_condition=" + sky_condition
				+ ", liqd_per_dept_dimen_hr=" + liqd_per_dept_dimen_hr + ", liqd_per_dept_dimen_6hr="
				+ liqd_per_dept_dimen_6hr + "]";
	}
	

}
