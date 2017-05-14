
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>
#include <cmath>
#include <iostream>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
# define RM_CREATE_TABLE_FAIL 1
# define RM_DELETE_TABLE_FAIL 2
# define RM_OPEN_FILE_FAIL 3
# define RM_DELETE_RECORD_FAIL 4
# define RM_UPDATE_RECORD_FAIL 5
# define RM_READ_RECORD_FAIL 6
# define RM_PRINT_RECORD_FAIL 7
# define RM_INSERT_RECORD_FAIL 8
# define RM_SYSTEM_CATALOG_ACCESS 9
# define RM_FILE_CLOSE_FAIL 10

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator();
  ~RM_ScanIterator();

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  RBFM_ScanIterator scanner;
  // FileHandle fileHandle;
private:
  static RecordBasedFileManager *_rbf_manager;
  
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbf_manager;
  int numberOfTables;

  vector<Attribute> createTableDescriptor();
  int getActualByteForNullsIndicator(int fieldCount);
  void prepareTableTuple(int attributeCount, unsigned char *nullAttributesIndicator, const int tableId, const int tableNameLength, 
      const string &tableName, const int fileNameLength, const string &fileName, const int userType, void *buffer, int *tupleSize);
  vector<Attribute> createColumnDescriptor();
  void prepareColumnTuple(int attributeCount, unsigned char *nullAttributesIndicator, const int tableId, const int columnNameLength, 
    const string &columnName, const int columnType, const int columnLength, const int columnPosition, void *buffer, int *tupleSize);
};

#endif
