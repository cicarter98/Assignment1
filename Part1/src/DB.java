import java.io.FileNotFoundException;
import java.util.Arrays;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;

public class DB {
  public static final int NUM_RECORDS = 67;
  public static final int RECORD_SIZE = 100;
  public static final int MAX_DATABASE_SIZE = 16;

  private RandomAccessFile Din;
  private int num_records;

  public DB() {
    this.Din = null;
    this.num_records = 0;
  }

  /**
   * Opens the file in read/write mode
   * 
   * @param filename (e.g., input.txt)
   * @return status true if operation successful
   */
  public void open(String filename) {
    // Set the number of records
    this.num_records = NUM_RECORDS;

    // Open file in read/write mode
    try {
      this.Din = new RandomAccessFile(filename, "rw");
    } catch (FileNotFoundException e) {
      System.out.println("Could not open file\n");
      e.printStackTrace();
    }
  }

  /**
   * Close the database file
   */
  public void close() {
    try {
      Din.close();
    } catch (IOException e) {
      System.out.println("There was an error while attempting to close the database file.\n");
      e.printStackTrace();
    }
  }

  /**
   * Get record number n (Records numbered from 0 to NUM_RECORDS-1)
   * 
   * @param record_num
   * @return values of the fields with the name of the field and
   *         the values read from the record
   */
  public Record readRecord(int record_num) {
    Record record = new Record();
    String[] fields;

    if ((record_num >= 0) && (record_num < this.num_records)) {
      try {
        Din.seek(0); // return to the top of the file
        Din.skipBytes(record_num * RECORD_SIZE);
        // parse record and update fields
        fields = Din.readLine().split("\\s+", 0);
        record.updateFields(fields);
      } catch (IOException e) {
        System.out.println("There was an error while attempting to read a record from the database file.\n");
        e.printStackTrace();
      }
    }

    return record;
  }

  /**
   * Binary Search by record id
   * 
   * @param id
   * @return Record number (which can then be used by read to
   *         get the fields) or -1 if id not found
   */
  public int binarySearch(String id) {
    int Low = 0;
    int High = NUM_RECORDS - 1;
    int Middle = 0;
    boolean Found = false;
    Record record;

    while (!Found && (High >= Low)) {
      Middle = (Low + High) / 2;
      record = readRecord(Middle);
      String MiddleId = record.Id;

      // int result = MiddleId[0].compareTo(id); // DOES STRING COMPARE
      int result = Integer.parseInt(MiddleId) - Integer.parseInt(id); // DOES INT COMPARE of MiddleId[0] and id
      if (result == 0)
        Found = true;
      else if (result < 0)
        Low = Middle + 1;
      else
        High = Middle - 1;
    }
    if (Found) {
      return Middle; // the record number of the record
    } else
      return -1;
  }

  public void createTestCSV(String filename)
  {
	  RandomAccessFile testCSVFile = null;
	  try
	  {
		  testCSVFile = new RandomAccessFile(filename, "rw");
	  }
	  catch(IOException e)
	  {
		  System.out.println(String.format("Could not create %s.csv", filename));
	  }
	  
	  String[] idArray = {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10"};
	  String[] statesArray = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GE"};
	  String[] cityArray = {"Montgomery", "Juneau", "Phoenix", "Little Rock", "Sacramento", "Denver", "Hartford", "Dover", "Tallahassee", "Atlanta"};
	  String[] nameArray = {"Person 1", "Person 2", "Person 3", "Person 4", "Person 5","Person 6", "Person 7", "Person 8", "Person 9", "Person 10"};
  
	  for (int i = 0; i < 10; i++)
	  {
		  Record r = new Record();
		  String[] fields = {idArray[i], statesArray[i], cityArray[i], nameArray[i]};
		  try
		  {
			  r.updateFields(fields);
		  }
		  catch (IOException e)
		  {
			  System.out.println("Fields array is not of length 4");
		  }
		  String csvString = String.format("%s,%s,%s,%s\n", r.Id, r.State, r.City, r.Name);
		  try
		  {
			  testCSVFile.write(csvString.getBytes());
		  }
		  catch (IOException e)
		  {
			  e.printStackTrace();
		  }
	  }
  }
  
  public boolean writeRecord(RandomAccessFile Dout, String id, String state, String city, String name)
  {
	  
	  Record r = new Record();
	  String[] fields = {id, state, city, name};
	  try
	  {
		  r.updateFields(fields);
	  }
	  catch(IOException e)
	  {
		  System.out.println("Could not create record from fields array, fields array should be of size 4");
		  return false;
	  }
	  
	  
	  byte[] recordByteArray = r.toByteArray();
	  if (recordByteArray.length > DB.RECORD_SIZE)
	  {
		  System.out.println("The concatenation of fields exceeded the buffer size of " + String.valueOf(DB.RECORD_SIZE));
		  throw new BufferOverflowException();

	  }
	  else
	  {
		  byte [] fixedLengthRecordByteArray = new byte[DB.RECORD_SIZE];
		  System.arraycopy(recordByteArray, 0, fixedLengthRecordByteArray, 0, DB.RECORD_SIZE - recordByteArray.length);
		  try
		  {
			  Dout.write(fixedLengthRecordByteArray);
			  return true;
		  }
		  catch(IOException e)
		  {
			  return false;
		  }
	  }
	  

  }
  
  public boolean writeRecord(RandomAccessFile Dout, String[] fields)
  {
	  
	  Record r = new Record();
	  try
	  {
		  r.updateFields(fields);
	  }
	  catch(IOException e)
	  {
		  System.out.println("Could not create record from fields array, fields array should be of size 4");
		  return false;
	  }
	  
	  byte[] recordByteArray = r.toByteArray();
	  
	  if (recordByteArray.length > DB.RECORD_SIZE)
	  {
		  System.out.println("The concatenation of fields exceeded the buffer size of " + String.valueOf(DB.RECORD_SIZE));
		  throw new BufferOverflowException();

	  }
	  else
	  {
		  byte [] fixedLengthRecordByteArray = new byte[DB.RECORD_SIZE];
		  Arrays.fill(fixedLengthRecordByteArray, (byte) 0x20);
		  
		  System.arraycopy(recordByteArray, 0, fixedLengthRecordByteArray, DB.RECORD_SIZE - recordByteArray.length, recordByteArray.length);
		  try
		  {
			  Dout.write(fixedLengthRecordByteArray);
			  return true;
		  }
		  catch(IOException e)
		  {
			  return false;
		  }
	  }
  }
  
  public Record[] parseCSV(String filename)
  {
	  open(filename);
	  RandomAccessFile csvFile = this.Din;
	  
	  if (csvFile != null)
	  {
		  Record[] parsedRecordsFromCSV = new Record[this.MAX_DATABASE_SIZE];
		  try 
		  {
			csvFile.seek(0);
		  } 
		  catch (IOException e1)
		  {
			e1.printStackTrace();
		  }
		  for (int i = 0; i < this.MAX_DATABASE_SIZE; i++)
		  {
			  try
			  {
				  String csvRecordStringParse = csvFile.readLine();
				  String[] csvRecordString = csvRecordStringParse.split(",");
				  Record r = new Record();
				  r.updateFields(csvRecordString);
				  parsedRecordsFromCSV[i] = r;
			  } 
			  catch (IOException e)
			  {
				e.printStackTrace();
			  }
		  }
		  
		  
		  return parsedRecordsFromCSV;
	  }
	  else
	  {
		  System.out.println(String.format("Could not access %s.csv", filename));
	  }
	  return null;
  }
  
  public void CreateConfigFile(int numberOfRecords)
  {
	  
  }
  
  
  public void CreateDB (Record[] records, String outputFilename)
  {
	  open(outputFilename);
	  RandomAccessFile outputFile = Din;
	  
	  if (outputFile != null)
	  {
		  for (Record r : records)
		  {
			  writeRecord(outputFile, r.getFields());
		  }
	  }
  }
  
  public void OpenDatabase(String filenameTripletPrefix)
  {
	  
  }
  
  
  public static void main(String[] args)
  {
	  // Mock code to test the writeRecord function
	  
	  /*
	  DB databse = new DB();
	  RandomAccessFile outputFile = null;
	  try
	  {
		  outputFile = new RandomAccessFile("test.rec", "rw");
	  }
	  catch (IOException e) 
	  {
		  System.out.println("Could not open file");
	  }
	  
	  if (outputFile != null)
	  {
		  databse.writeRecord(outputFile, "00", "Arkansas", "Bentonville", "Christopher Carter");
	  }
	  
	  // Test was successful
	  */
	  
	  System.out.println("200697,Ohio,Dayton,Air_Force_Institute_of_Technology-Graduate_School_of_Engineering_&_Management".length());
	  
	  DB database = new DB();
	  database.CreateDB(database.parseCSV("colleges-lf.csv"), "output.txt");
	 
  }
}
