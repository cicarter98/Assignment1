import java.io.IOException;
import java.nio.BufferOverflowException;

public class Record {

  private boolean empty;

  public String Id;
  public String State;
  public String City;
  public String Name;
  
  private int[] fieldFixedSizes;

  public Record() {
    empty = true;
    this.fieldFixedSizes = new int[] {7, 15, 20, DB.RECORD_SIZE - 7 - 15 - 20 - 2};
  }

  /**
   * Update the fields of a record from an array of fields
   * 
   * @param fields array with values of fields
   * @return nothing
   * @throws IOException
   */
  
  public void updateFields(String[] fields) throws IOException {
	  if (fields.length == 4) 
	  {
    	if (fields[0].length() <= this.fieldFixedSizes[0])
    		this.Id = fields[0] + " ".repeat(this.fieldFixedSizes[0] - fields[0].length());
    	else
    	{
    		System.out.println("Could not fit data in field: ID field is fixed  at size " + String.valueOf(this.fieldFixedSizes[0]));
    		throw new BufferOverflowException();
    	}
    	
    	if (fields[1].length() <= this.fieldFixedSizes[1])
    		this.State = fields[1] + " ".repeat(this.fieldFixedSizes[1] - fields[1].length());
    	else
    	{
    		System.out.println("Could not fit data in field: State field is fixed  at size " + String.valueOf(this.fieldFixedSizes[1]));
    		throw new BufferOverflowException();
    	}
    	
    	if (fields[2].length() <= this.fieldFixedSizes[2])
    		this.City = fields[2] + " ".repeat(this.fieldFixedSizes[2] - fields[2].length());
    	else
    	{
    		System.out.println("Could not fit data in field: City field is fixed  at size " + String.valueOf(this.fieldFixedSizes[2]));
    		throw new BufferOverflowException();
    	}
    	
    	if (fields[3].length() <= this.fieldFixedSizes[3])
    		this.Name = fields[3] + " ".repeat(this.fieldFixedSizes[3] - fields[3].length());
    	else
    	{
    		System.out.println("Could not fit data in field: Name field is fixed  at size " + String.valueOf(this.fieldFixedSizes[3]));
    		System.out.println(fields[3]);
    		throw new BufferOverflowException();
    	}
		empty = false;
	  } 
	  else
		  throw new IOException();
  }
  
  public String[] getFields()
  {
	  String[] fields = {this.Id, this.State, this.City, this.Name};
	  return fields;
  }

  /**
   * Check if record fields have been updated
   * 
   * @return true if record has been updated otherwise false
   */
  public boolean isEmpty() {
    return empty;
  }

  public String toString() {
    return "Id: " + this.Id +
        ", State: " + this.State +
        ", City: " + this.City +
        ", Name: " + this.Name;
  }
  
  
  public byte[] toByteArray()
  {
	  byte[] recordString = String.format("%s%s%s%s\n", this.Id, this.State, this.City, this.Name).getBytes();
	  return recordString;
  }
  


}
