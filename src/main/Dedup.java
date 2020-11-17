package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Dedup {
	
	//stores the actual data chunks
	public ArrayList<byte[]> deduplicatedFileChunks;
	
	//for each file stores pointers to the chunks in the fileContents arrayList.
	//this object and the fileContents objects are the compressed format
	public HashMap<String,FileMetaData> fileInfoObjects;
	
	/*
	 * this object uses the hash of the chunks seen so far
	 * to enable efficient detection of previously seen chunks.
	 * for each chunk it maintains the position in fileContents
	 * where the chunk can be found. The byte array chunks are converted
	 * to BigInteger which is used as a key for the hash map. 
	 */
	private HashMap<BigInteger, Integer> indicesHashMap;
	private HashMap<BigInteger, Integer> chunkFreqHashMap;
	
	private long actualContentSize = 0;
	private int numDedupedBlocks =0;
	int chunkSize =0;
	int numFiles =0;
	public double deltaDedupFactor =0;
	public int maxChunkPopularity =0;
	public Dedup(int chunkSize) {
		deduplicatedFileChunks = new ArrayList<byte[]>();
		fileInfoObjects = new HashMap<String,FileMetaData>();
		indicesHashMap = new HashMap<BigInteger, Integer>();
		chunkFreqHashMap = new HashMap<BigInteger, Integer>();
		this.chunkSize = chunkSize;
	}
	
	/*
	 * this function splits each file into chunks. If a chunk is already present 
	 * in fileContents, then a pointer to this chunk is maintained, else the chunk is
	 * added to fileContents. Checking the presence is done using the hashMap. 
	 */
	public void processFile(File file){
		try {
			/*
			 * the file meta data object is a simple file place holder
			 * for its name and positions of each of its chunks
			 */
			if(Util.debug)System.out.println(file.getName());
			byte[] fileBytes = Util.readBytesFromFile(file);
			FileMetaData fileInfo = new FileMetaData(file.getName(),fileBytes.length);

			//TODO the above step is not efficient. Ideally read it byte by byte. 
			if(fileBytes.length < chunkSize)				
					throw new Exception("File "+file.getName()+" is smaller than chunk size!");
			
			//routine to read the byte array of the files into chunk sizes.
			int offset = 0;
			while (offset < fileBytes.length) {
				byte[] outputBytes;
				if(fileBytes.length - offset < chunkSize ) {
					outputBytes = new byte[fileBytes.length - offset];
					System.arraycopy(fileBytes, offset, outputBytes, 0, fileBytes.length - offset);
				}
				else{
					outputBytes = new byte[chunkSize];
					System.arraycopy(fileBytes, offset, outputBytes, 0, chunkSize);
				}	
				BigInteger dataChunk = new BigInteger(outputBytes);
				offset +=chunkSize ; 
			
				//obtain pointer to the chunk
				Integer chunkIndex = indicesHashMap.get(dataChunk);
				if(chunkIndex == null){
					int index = deduplicatedFileChunks.size();
					deduplicatedFileChunks.add(outputBytes);
					actualContentSize = actualContentSize + (dataChunk.toByteArray().length);
					indicesHashMap.put(dataChunk, index);
					fileInfo.positionOfEachChunk.add(index);
					chunkFreqHashMap.put(dataChunk, 1);
				}else{
					fileInfo.positionOfEachChunk.add(chunkIndex.intValue());
					numDedupedBlocks = numDedupedBlocks + 1;
					
					int noOfApp = chunkFreqHashMap.get(dataChunk);
					chunkFreqHashMap.put(dataChunk, noOfApp+1);
				}
			}
			fileInfoObjects.put(file.getName(),fileInfo);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	/*
	 * takes the files in the inputDir and compresses them by identifying identical chunks
	 * the real work is done in processFile function.
	 */
	public long compress(File[] listOfFiles){
		numFiles = listOfFiles.length;
		for (int i = 0; i < listOfFiles.length; i++) {
		  File file = listOfFiles[i];
		  if(file.getName().startsWith("."))
			  continue;//don't want to deal with hidden files. 
		  else
			  processFile(file);
		  
		}	
		return actualContentSize;
	}
	
	/*
	 * decompresses the files in the folder dedupDirectoryName and puts them in the folder
	 * deCompressTarget
	 */
	@SuppressWarnings("unchecked")
	public void deCompress(String dedupDirectoryName, String deCompressTarget){
		ArrayList<byte[]> reCreatedfileContents;
		HashMap<String,FileMetaData> reCreatedfileInfoObjects;
		//first deserialize the compressed objects and obtain the necessary file objects
		FileInputStream fis = null;
		ObjectInputStream in = null;
		try {
			fis = new FileInputStream(dedupDirectoryName+"/dedupCore");
			in = new ObjectInputStream(fis);
			reCreatedfileContents = (ArrayList<byte[]>) in.readObject();
			if(Util.debug)System.out.println("Recreated file contents:"+ reCreatedfileContents);
			in.close();		
			fis = new FileInputStream(dedupDirectoryName+"/fileInfo");
			in = new ObjectInputStream(fis);
			reCreatedfileInfoObjects = (HashMap<String,FileMetaData>) in.readObject();
			if(Util.debug)System.out.println("Recreated file contents:"+ reCreatedfileInfoObjects);
			in.close();
			
			//given the file content and the file info objects reCreates all the files.
			reCreateFiles(reCreatedfileContents,reCreatedfileInfoObjects,deCompressTarget);
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		}	}
	
	/*
	 * For each file, the file info object provides of the location of each chunk in the file
	 * present in the file content array list. Just a question of assembling those chunks,
	 * converting it into a byte array and writing it back to the file system. 
	 */
	public void reCreateFiles(ArrayList<byte[]> reCreatedfileContents,HashMap<String,FileMetaData> reCreatedfileInfoObjects,String deCompressTarget){
		for(FileMetaData fMetaData: reCreatedfileInfoObjects.values()){
			int numberOfChunks = fMetaData.numberOfChunks();
			byte[] completeFileByteArray = new byte[fMetaData.totalFileLength];
			int pointer=0;
			for(int j=0; j < numberOfChunks;++j){
				int actualPositionOfChunk = fMetaData.actualPositionOfChunk(j);
				byte[] tempBytes = reCreatedfileContents.get(actualPositionOfChunk);
				for(int  k=0; k < tempBytes.length;++k){
					completeFileByteArray[pointer] = tempBytes[k];
					pointer++;
				}
			}
			File out = new File(deCompressTarget+"/"+fMetaData.getName());
			try { 
				Util.writeBytesToFile(out, completeFileByteArray);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//	System.out.println();
		}	
	}
	public byte[] getFileBytes(String fileName){
		FileMetaData fMetaData = fileInfoObjects.get(fileName);
		int numberOfChunks = fMetaData.numberOfChunks();
		byte[] completeFileByteArray = new byte[fMetaData.totalFileLength];
		int pointer=0;
		for(int j=0; j < numberOfChunks;++j){
			int actualPositionOfChunk = fMetaData.actualPositionOfChunk(j);
			byte[] tempBytes = deduplicatedFileChunks.get(actualPositionOfChunk);
			for(int  k=0; k < tempBytes.length;++k){
				completeFileByteArray[pointer] = tempBytes[k];
				pointer++;
			}
		}
		return completeFileByteArray;
	}
	
	public File reGenerateFile(String fileName){
		byte[] completeFileByteArray = getFileBytes(fileName);
		FileMetaData fMetaData = fileInfoObjects.get(fileName);
		File out = new File(fMetaData.getName()+"*");
		try {
			Util.writeBytesToFile(out, completeFileByteArray);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return out; 
	}
	
	/*
	 * this function is only for debug purposes. Compares the original files with the ones
	 * recreated after decompression. 
	 */
	public boolean authenticateDedup(String originalFileFolder, String reCreatedFileFolder){
		try {
			File folder = new File(originalFileFolder);
			File[] listOfFiles = folder.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
			  File origFile = listOfFiles[i];
			  if(origFile.getName().startsWith("."))
				  continue;
			  byte[] origBArray = Util.readBytesFromFile(origFile);
			  File reCreatedFile = new File(reCreatedFileFolder+"/"+origFile.getName());//we recreate with the same names
			  byte[] reCreatedByteArray = Util.readBytesFromFile(reCreatedFile);
			  if(!Arrays.equals(origBArray, reCreatedByteArray)){
				  throw new Exception(origFile.getName()+" does not match!");
			  }
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return true; 
	}
	
	public void writeDedupInfoToFile(String directoryName){
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		if(Util.debug)System.out.println("original file contents:"+ deduplicatedFileChunks);
		if(Util.debug)System.out.println("original metadata contents:"+ fileInfoObjects);

		try {
			//first write the actual file contents
			fos = new FileOutputStream(directoryName+"/dedupCore");
			out = new ObjectOutputStream(fos);
			out.writeObject(deduplicatedFileChunks);
			out.close();		
			fos.close();
			//write the file meta data info needed to decompress files
			fos = new FileOutputStream(directoryName+"/fileInfo");
			out = new ObjectOutputStream(fos);
			
			out.writeObject(fileInfoObjects);
			out.close();
			fos.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	public void genChunkFreq(){
		Collection<Integer> freqVector = chunkFreqHashMap.values();
		HashMap<Integer, Integer> histoGram = new HashMap<Integer, Integer>();
		for (Integer freq : freqVector) {
			if(histoGram.containsKey(freq)){
				int value = histoGram.get(freq);
				value = value +1;
				histoGram.put(freq,value);
			}
			else
				histoGram.put(freq,1);
		}	
		int totalNumChunks = 0;
		for (Map.Entry<Integer, Integer> entry : histoGram.entrySet()) {
			totalNumChunks = totalNumChunks + entry.getValue();
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
		deltaDedupFactor=0;//reinitializing to be on the safer side.
		maxChunkPopularity =0;
	//	System.out.println("toaNumChunks:"+totalNumChunks);
	//	System.out.println("numFiles:"+numFiles);

		for (Map.Entry<Integer, Integer> entry : histoGram.entrySet()) {
			double p = entry.getKey();
			double frac = (double)entry.getValue()/totalNumChunks;
		//	System.out.println("frac:"+frac);
			double factor1 = (double) (p*p - p)/numFiles;
		//	System.out.println("factor 1:"+factor1);

			double factor2 = p - factor1;
		//	System.out.println("factor 2:"+factor2);

			deltaDedupFactor = deltaDedupFactor + frac*factor2;
		//	System.out.println("del-ded"+ deltaDedupFactor);
			if(p > maxChunkPopularity)
				maxChunkPopularity =(int)p;
		}
		//System.out.println("factor:"+ deltaDedupFactor+" maxChunk:"+maxChunkPopularity);
		
	 }
	/*
	 * the main function takes a set of files and 1) compresses them, 2) serializes the
	 * compressed output 3) decompresses the compressed output, writes it to a new folder
	 * 4) compares the original files with the recreated ones. 
	 */
	public static void main(String[] args) {
		int maxChunkSize = 1000;
		String dedupSource = "input";//location of input files
		String compTarget = "output";//location where the deduped files must be written to
		String deCompTarget = "output";//location where the decompressed files must be written to
		//int maxChunkSize = Integer.parseInt(args[0]);
		//String dedupSource = args[1];//location of input files
		//String compTarget = args[2];//location where the deduped files must be written to
		//String deCompTarget = args[3];//location where the decompressed files must be written to
		//for(int i=0; i < 100; ++i){
			Util.deleteFolderContent(new File(compTarget));
			Util.deleteFolderContent(new File(deCompTarget));
			int chunkSize=1000;
			//int chunkSize = Util.getRandNumberInRange(100, maxChunkSize); 
			Dedup dp = new Dedup(chunkSize);
			File f = new File(dedupSource);
			dp.compress(f.listFiles());
			dp.writeDedupInfoToFile(compTarget);
			dp.deCompress(compTarget,deCompTarget);
			dp.authenticateDedup(dedupSource,deCompTarget);
			System.out.println("Dedup Successfull!, Chunk Size:"+chunkSize);

			File originalFiles = new File(dedupSource);
			long originalSize = Util.folderSize(originalFiles)/1000;
			System.out.println("Before compression:"+originalSize+"KB");

			File compressedFiles = new File(compTarget);
			long compSize = Util.folderSize(compressedFiles)/1000;
			System.out.println("After compression:"+compSize+"KB");
			
			long percentComp = ((originalSize-compSize)*100)/originalSize;
			System.out.println("Percentage Compression:"+ percentComp+"%");

			System.out.println("Actual core content size:"+dp.actualContentSize/1000+"KB"+ " | No. of Duplicate blocks:"+dp.numDedupedBlocks);
			System.out.println("---------------------------------");

		//}
	}

}
