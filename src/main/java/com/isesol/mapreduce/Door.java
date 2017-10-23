package com.isesol.mapreduce;



public abstract class Door {

	public static String open(){
		System.out.println("the door is open");
		return "aaa";
	};
	abstract void close();
	abstract String getName();
}
