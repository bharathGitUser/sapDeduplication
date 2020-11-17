package main;


	import java.io.BufferedOutputStream;
	import java.io.File;
	import java.io.FileInputStream;
	import java.io.FileOutputStream;
	import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

	public class Util{
		
		//variables that need to be set
		public static final String referenceFileName = "refFile";
		public static boolean debug = false; 

		
		public static final int numServers = 7;
		public static final String[] serverHostNames = {"master","slave1","slave1","slave1","slave3","slave3","slave3"};
		public static final int[] serverPorts = {5000,5001,5002,5003,5004,5005,5006,5007};

		//public static final String[] serverHostNames = {"master","slave1","slave3"};
		//public static final int[] serverPorts = {5000,5001,5002};

		public static final String clientHostName = "master";
		public static final int clientPort = 6000;
		
		public static  int chunkSize = 3;
		
		//NOTE: you also need to change the path in the addpath command (first line) in mytest.m 
		public final static String matlabExecPath = "/home/hduser/MATLAB/R2012a/bin/matlab -nodisplay -nodesktop  -nosplash";
		public final static String optBranchingPath = "/home/hduser/Dropbox/academics/Princeton/CloudComputing/CorrelatedStorage/code/optBranch/";
		public final static String matlabCommand = matlabExecPath + "<"+ optBranchingPath+"myTest.m > out";
		public final static String optBranchInputFilePath = optBranchingPath +"graphInput.txt";
		//end of variables that need to be set
		
	     /** 
	     * Read bytes from a File into a byte[].
	     * 
	     * @param file The File to read.
	     * @return A byte[] containing the contents of the File.
	     * @throws IOException Thrown if the File is too long to read or couldn't be
	     * read fully.
	     */
	    public static byte[] readBytesFromFile(File file) throws IOException {
	      InputStream is = new FileInputStream(file);
	      
	      // Get the size of the file
	      long length = file.length();
	  
	      // You cannot create an array using a long type.
	      // It needs to be an int type.
	      // Before converting to an int type, check
	      // to ensure that file is not larger than Integer.MAX_VALUE.
	      if (length > Integer.MAX_VALUE) {
	        throw new IOException("Could not completely read file " + file.getName() + " as it is too long (" + length + " bytes, max supported " + Integer.MAX_VALUE + ")");
	      }
	  
	      // Create the byte array to hold the data
	      byte[] bytes = new byte[(int)length];
	  
	      // Read in the bytes
	      int offset = 0;
	      int numRead = 0;
	      while (offset < bytes.length && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
	          offset += numRead;
	      }
	  
	      // Ensure all the bytes have been read in
	      if (offset < bytes.length) {
	          throw new IOException("Could not completely read file " + file.getName());
	      }
	  
	      // Close the input stream and return bytes
	      is.close();
	      return bytes;
	  }
	    
	    /**
	     * Writes the specified byte[] to the specified File path.
	     * 
	     * @param theFile File Object representing the path to write to.
	     * @param bytes The byte[] of data to write to the File.
	     * @throws IOException Thrown if there is problem creating or writing the 
	     * File.
	     */
	    public static void writeBytesToFile(File theFile, byte[] bytes) throws IOException {
	      BufferedOutputStream bos = null;
	      
	    try {
	      FileOutputStream fos = new FileOutputStream(theFile);
	      bos = new BufferedOutputStream(fos); 
	      bos.write(bytes);
	    }finally {
	      if(bos != null) {
	        try  {
	          //flush and close the BufferedOutputStream
	          bos.flush();
	          bos.close();
	        } catch(Exception e){}
	      }
	    }
	    }
	    
	    public static long folderSize(File directory) {
	        long length = 0;
	        for (File file : directory.listFiles()) {
	            if (file.isFile())
	                length += file.length();
	            else
	                length += folderSize(file);
	        }
	        return length;
	    }
	    
	    public static int getRandNumberInRange(int Min, int Max){
	    	return Min + (int)(Math.random() * ((Max - Min) + 1));
	    }
	    
		static <T> List<List<T>> chopped(List<T> list, final int L) {
		    List<List<T>> parts = new ArrayList<List<T>>();
		    final int N = list.size();
		    for (int i = 0; i < N; i += L) {
		        parts.add(new ArrayList<T>(
		            list.subList(i, Math.min(N, i + L)))
		        );
		    }
		    return parts;
		}

	    public static void deleteFolderContent(File folder) {
	        File[] files = folder.listFiles();
	        if(files!=null) { //some JVMs return null for empty dirs
	            for(File f: files) {
	                if(f.isDirectory()) {
	                    deleteFolderContent(f);
	                } else {
	                    f.delete();
	                }
	            }
	        }
	      //  folder.delete();
	    }
		public static void main(String[] args) {
			File file1 = new File("/Users/bharath/reCreated");
			deleteFolderContent(file1);
			
			//FileUtils.deleteDirectory(file1);

		
		}
		
		public static boolean areFilesEqual(File a, File b){
			try {
				byte[] aBytes = Util.readBytesFromFile(a);
				byte[] bBytes = Util.readBytesFromFile(b);
				if(aBytes.length != bBytes.length)
					return false;
				for(int i =0; i < aBytes.length;++i)
					if(aBytes[i] != bBytes[i])
						return false;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return true;
		}

	}	



	