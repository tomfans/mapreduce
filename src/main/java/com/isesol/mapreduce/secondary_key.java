package com.isesol.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class secondary_key implements Writable{
	
	private int avg;
	private int count;


	public void setAvg(int avg){
		
		this.avg = avg;
	}
	
	public int getAvg(){
		
		return this.avg;
	}
	
	
	
	public void setCount(int count){
		this.count = count;
	}
	
	public int getCount(){
		return count;
	}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.getAvg());
		out.writeInt(this.getCount());

	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		avg = in.readInt();
		count = in.readInt();

	}
	
	public String toString(){
		return avg + "-" + count;
	}

}
