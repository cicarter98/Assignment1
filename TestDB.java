public class TestDB {
  static Record record;

  public static void main(String[] args) {

    // calls constructor
    DB db = new DB();

    // opens "input.txt"
    db.open("input.txt");

    System.out
        .println("------------- Testing readRecord ------------");

    // Reads record 0
    // Then prints the values of the 5 fields to the screen with the name of the
    // field and the values read from the record, i.e.,
    // id: 00003 experience: 3 married: no wages: 1.344461678 industry:
    // Business_and_Repair_Service
    int record_num = 0;
    record = new Record();
    record = db.readRecord(record_num);
    if (!record.isEmpty())
      System.out.println("RecordNum " + record_num + ": " + record.toString() + "\n\n");
    else {
      System.out.println("Could not get Record " + record_num);
      System.out.println("Record out of range");
    }

    // Reads record 66 (last record)
    record_num = DB.NUM_RECORDS - 1;
    record = db.readRecord(record_num);
    if (!record.isEmpty())
      System.out.println("RecordNum " + record_num + ": " + record.toString() + "\n\n");
    else {
      System.out.println("Could not get Record " + record_num);
      System.out.println("Record out of range");
    }

    System.out
        .println("------------- Testing binarySearch ------------");

    // Find record 17
    String ID = "17";
    record_num = db.binarySearch(ID);
    if (record_num != -1) {
      record = db.readRecord(record_num);
      System.out
          .println(
              "ID " + ID + " found at Record " + record_num + "\nRecordNum " + record_num + ": \n" + record.toString()
                  + "\n\n");
    } else
      System.out.println("ID " + ID + " not found in our records\n\n");

    // Find record 0000
    ID = "0000";
    record_num = db.binarySearch(ID);
    if (record_num != -1) {
      record = db.readRecord(record_num);
      System.out
          .println(
              "ID " + ID + " found at Record " + record_num + "\nRecordNum " + record_num + ": \n" + record.toString()
                  + "\n\n");
    } else
      System.out.println("ID " + ID + " not found in our records\n\n");

    // Find record 00113
    ID = "00113";
    record_num = db.binarySearch(ID);
    if (record_num != -1) {
      record = db.readRecord(record_num);
      System.out
          .println(
              "ID " + ID + " found at Record " + record_num + "\nRecordNum " + record_num + ": \n" + record.toString()
                  + "\n\n");
    } else
      System.out.println("ID " + ID + " not found in our records\n\n");

    // Find record 00104
    // Prints error message
    ID = "00104";
    record_num = db.binarySearch(ID);
    if (record_num != -1) {
      record = db.readRecord(record_num);
      System.out
          .println(
              "ID " + ID + " found at Record " + record_num + "\nRecordNum " + record_num + ": \n" + record.toString()
                  + "\n\n");
    } else
      System.out.println("ID " + ID + " not found in our records\n\n");

    // Close database
    db.close();
  }
}
