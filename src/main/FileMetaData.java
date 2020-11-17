package main;


import java.io.Serializable;
import java.util.ArrayList;

public class FileMetaData implements Serializable{
	private String name;
	public int totalFileLength;
	public ArrayList<Integer> positionOfEachChunk;
	public FileMetaData(String fileName,int totalFileLength){
		positionOfEachChunk = new ArrayList<Integer>();
		name = fileName;
		this.totalFileLength = totalFileLength;
	}
	public String getName() {
		return name;
	}

	public int numberOfChunks(){
		return positionOfEachChunk.size();
	}
	
	public String toString(){
		return name+","+positionOfEachChunk+"\n";
	}
	public int actualPositionOfChunk(int index){
		return positionOfEachChunk.get(index);
	}
}
