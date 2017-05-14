
#include "rm.h"

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager *RM_ScanIterator::_rbf_manager = NULL;
RecordBasedFileManager *RelationManager::_rbf_manager = NULL;

RM_ScanIterator::RM_ScanIterator()
{
    _rbf_manager = RecordBasedFileManager::instance();
    FileHandle fileHandle;
}

RM_ScanIterator::~RM_ScanIterator()
{ 
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){


    if(scanner.getNextRecord(rid, data) == RBFM_EOF){
        return RM_EOF;
    }else{
        return SUCCESS;
    }
}

RC RM_ScanIterator::close(){
    if(_rbf_manager->closeFile(fileHandle)){
        return RM_FILE_CLOSE_FAIL;
    }else{
        return SUCCESS;
    }
}

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    // Initialize the internal RecordBasedFileManager instance
    _rbf_manager = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    vector<Attribute> tableAttr = createTableDescriptor();
    if(_rbf_manager->createFile("Tables")){
        return RM_CREATE_TABLE_FAIL;
    }

    vector<Attribute> columnAttr = createColumnDescriptor();
    if(_rbf_manager->createFile("Columns"))
        return RM_CREATE_TABLE_FAIL;

    void *record = malloc(1000);
    int recordSize = 0;
    RID rid;
//start to fill "Tables" table

    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(tableAttr.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    FileHandle tableHandle;
    if(_rbf_manager->openFile("Tables", tableHandle))
        return RM_OPEN_FILE_FAIL;
    prepareTableTuple(tableAttr.size(),nullsIndicator, 1, 6, "Tables", 6, "Tables", 0, record,&recordSize);
    _rbf_manager->insertRecord(tableHandle,tableAttr, record, rid);
    memset(record, 0, 1000);

    prepareTableTuple(tableAttr.size(),nullsIndicator, 2, 7, "Columns", 7, "Columns", 0, record,&recordSize);
    _rbf_manager->insertRecord(tableHandle, tableAttr, record, rid);

    memset(record, 0, 1000);
//start to fill "Columns" table
    nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(columnAttr.size());
    free(nullsIndicator);
    unsigned char *newNullsIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
    memset(newNullsIndicator, 0, nullFieldsIndicatorActualSize);

    FileHandle columnHandle;
    if(_rbf_manager->openFile("Columns", columnHandle))
        return RM_OPEN_FILE_FAIL;
    for(unsigned i = 0; i<tableAttr.size();i++){
        prepareColumnTuple(columnAttr.size(), newNullsIndicator, 1, tableAttr[i].name.size(),tableAttr[i].name,
            tableAttr[i].type,tableAttr[i].length, i+1, record, &recordSize);
        _rbf_manager->insertRecord(columnHandle, columnAttr, record, rid);
        memset(record, 0, 1000);
    }

    for(unsigned i =0; i<columnAttr.size();i++){
        prepareColumnTuple(columnAttr.size(), newNullsIndicator, 2, columnAttr[i].name.size(),columnAttr[i].name,
            columnAttr[i].type,columnAttr[i].length, i+1, record, &recordSize);
        _rbf_manager->insertRecord(columnHandle, columnAttr, record, rid);
        memset(record, 0, 1000);
    }

    numberOfTables = 2;
    if(_rbf_manager->closeFile(tableHandle))
        return RM_FILE_CLOSE_FAIL;
    if(_rbf_manager->closeFile(columnHandle))
        return RM_FILE_CLOSE_FAIL;
    free(newNullsIndicator);
    free(record);
    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    if(_rbf_manager->destroyFile("Tables"))
        return RM_DELETE_TABLE_FAIL;

    if(_rbf_manager->destroyFile("Columns"))
        return RM_DELETE_TABLE_FAIL;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

//create the table
    if(_rbf_manager->createFile(tableName)){
cout<<"Entering CreateTable"<<endl;
        return RM_CREATE_TABLE_FAIL;
    }

    void * record= malloc(1000);
    int recordSize = 0;
    RID rid;
//insert the table into "Tables"
    vector<Attribute> tableAttr = createTableDescriptor();

    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(tableAttr.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    prepareTableTuple(tableAttr.size(),nullsIndicator, numberOfTables+1, tableName.size(), tableName, tableName.size(), tableName, 1, record,&recordSize);
    insertTuple("Tables", record, rid);
    memset(record, 0, 1000);
//insert the table into "Columns"
    vector<Attribute> columnAttr = createColumnDescriptor();
    nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(columnAttr.size());
    free(nullsIndicator);

    unsigned char *newNullsIndicator = (unsigned char*)malloc(nullFieldsIndicatorActualSize);
    memset(newNullsIndicator, 0, nullFieldsIndicatorActualSize);
    for(unsigned i = 0; i<attrs.size();i++){
        prepareColumnTuple(columnAttr.size(), newNullsIndicator, numberOfTables+1, attrs[i].name.size(),attrs[i].name,
            attrs[i].type,attrs[i].length, i+1, record, &recordSize);
        insertTuple("Columns", record, rid);
        memset(record, 0, 1000);
    }
    numberOfTables ++;
    free(newNullsIndicator);
    free(record);
    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if(tableName =="Tables" || tableName =="Columns")       
        return RM_SYSTEM_CATALOG_ACCESS;
    return _rbf_manager ->destroyFile(tableName);
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{   
    FileHandle tableHandle;
    if(_rbf_manager->openFile("Tables",tableHandle)) {     
         return RM_OPEN_FILE_FAIL;
    }   

    FileHandle columnHandle;
    if(_rbf_manager->openFile("Columns", columnHandle))
        return RM_OPEN_FILE_FAIL;    
       
    RID rid;
    void* tableIdBuffer = malloc(sizeof(int));

    int lengthTableName = tableName.length();
    void* nameBuffer = malloc(sizeof(int) + tableName.length());
    memcpy(nameBuffer, &lengthTableName, sizeof(int));
    memcpy((char*)nameBuffer+sizeof(int), (char*)tableName.c_str(), tableName.length());

    vector<Attribute> tableDescriptor= createTableDescriptor();
    vector<string> tableIds;
    tableIds.push_back("table-id");
    RBFM_ScanIterator rbfm_ScanIterator;
    _rbf_manager->scan(tableHandle,tableDescriptor, "table-name", EQ_OP,nameBuffer,tableIds,rbfm_ScanIterator);     //scans all the tables and returns their ids
    memset(tableIdBuffer, 0, sizeof(int));
    rbfm_ScanIterator.getNextRecord(rid, tableIdBuffer);
    
    unsigned targetId = *((int*)((char*)tableIdBuffer+1));
    rbfm_ScanIterator.close();

    vector<Attribute> columnDescriptor= createColumnDescriptor();
    void* gotAttribute = malloc(100);
    vector<string> attributes;
    attributes.push_back("column-type");
    attributes.push_back("column-name");
    RBFM_ScanIterator attributeGetter;
    _rbf_manager->scan(columnHandle, columnDescriptor, "table-id", EQ_OP, &targetId, attributes,attributeGetter );
    while(attributeGetter.getNextRecord(rid,gotAttribute)!=RM_EOF){
        Attribute add;
        int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributes.size());
        unsigned offset = nullAttributesIndicatorActualSize;

        int attributeType;
        memcpy(&attributeType, (char*)gotAttribute + offset, sizeof(int));
        if(attributeType == 0){
            add.type = TypeInt;
            add.length = (AttrType)4;
        }else if(attributeType == 1){
            add.type = TypeReal;
            add.length = (AttrType)4;
        }else{
            add.type = TypeVarChar;
            add.length = (AttrType)50;
        }

        offset+=sizeof(int);

        unsigned lengthVarChar;
        memcpy(&lengthVarChar, (char*)gotAttribute + offset, sizeof(int));
        offset+=sizeof(int);
        char testArray[lengthVarChar+1];
        memcpy(testArray, (char*)gotAttribute+offset, lengthVarChar);
        testArray[lengthVarChar] = '\0';
        add.name=testArray;
        
        attrs.push_back(add);
    }
    if(_rbf_manager->closeFile(tableHandle))
        return RM_FILE_CLOSE_FAIL;
    if(_rbf_manager->closeFile(columnHandle))
        return RM_FILE_CLOSE_FAIL;
    free(tableIdBuffer);
    free(nameBuffer);
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    FileHandle fileHandle;
    if(_rbf_manager->openFile(tableName, fileHandle))
        return RM_OPEN_FILE_FAIL;
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    if(_rbf_manager->insertRecord(fileHandle, attr, data, rid))
        return RM_INSERT_RECORD_FAIL;
    if(_rbf_manager->closeFile(fileHandle))
        return RM_FILE_CLOSE_FAIL;
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    if(tableName =="Tables" || tableName =="Columns")        
        return RM_SYSTEM_CATALOG_ACCESS;
    FileHandle fileHandle;
    if(_rbf_manager->openFile(tableName, fileHandle))
        return RM_OPEN_FILE_FAIL;
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    if(_rbf_manager->deleteRecord(fileHandle, attr, rid))
        return RM_DELETE_RECORD_FAIL;
    if(_rbf_manager->closeFile(fileHandle))
        return RM_FILE_CLOSE_FAIL;
    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fileHandle;
    if(_rbf_manager->openFile(tableName, fileHandle))
        return RM_OPEN_FILE_FAIL;
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    if(_rbf_manager->updateRecord(fileHandle, attr, data, rid))
        return RM_UPDATE_RECORD_FAIL;
    if(_rbf_manager->closeFile(fileHandle))
        return RM_FILE_CLOSE_FAIL;
    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fileHandle;
    if(_rbf_manager->openFile(tableName, fileHandle)){
        return RM_OPEN_FILE_FAIL;
    }
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    if(_rbf_manager->readRecord(fileHandle, attr, rid, data))
        return RM_READ_RECORD_FAIL;
    if(_rbf_manager->closeFile(fileHandle))
        return RM_FILE_CLOSE_FAIL;
    return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	if(_rbf_manager->printRecord(attrs,data))
        return RM_PRINT_RECORD_FAIL;
    return SUCCESS;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fileHandle;
    if(_rbf_manager->openFile(tableName, fileHandle))
        return RM_OPEN_FILE_FAIL;
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    if(_rbf_manager->readAttribute(fileHandle, attr, rid, attributeName, data))
        return RM_READ_RECORD_FAIL;
    if(_rbf_manager->closeFile(fileHandle))
        return RM_FILE_CLOSE_FAIL;
    return SUCCESS;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    if(_rbf_manager->openFile(tableName,rm_ScanIterator.fileHandle))
        return RM_OPEN_FILE_FAIL;
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    _rbf_manager->scan(rm_ScanIterator.fileHandle, attr,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.scanner);
    return SUCCESS;
}

// Returns the file descriptor for the Table catalog entry
vector<Attribute> RelationManager::createTableDescriptor()
{
    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attrs.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attrs.push_back(attr);

    attr.name = "user-type";       //0 if system, 1 if user
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    return attrs;
}

int RelationManager::getActualByteForNullsIndicator(int fieldCount) {

    return ceil((double) fieldCount / CHAR_BIT);
}

// Function to prepare the data in the correct form to be inserted/read/updated
void RelationManager::prepareTableTuple(int attributeCount, unsigned char *nullAttributesIndicator, const int tableId, const int tableNameLength, 
    const string &tableName, const int fileNameLength, const string &fileName, const int userType, void *buffer, int *tupleSize)
{
    int offset = 0;

	// Null-indicators
    bool nullBit = false;
    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

	// Null-indicator for the fields
    memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
	offset += nullAttributesIndicatorActualSize;

	// Beginning of the actual data    
	// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
	// e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

	// Is the table-id field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 7);

    if (!nullBit) {
		memcpy((char *)buffer + offset, &tableId, sizeof(int));
		offset += sizeof(int);
	}
	
	// Is the table-name field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 6);

    if (!nullBit) {
		memcpy((char *)buffer + offset, &tableNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, tableName.c_str(), tableNameLength);
		offset += tableNameLength;
	}
	
	
	// Is the file-name field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 5);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &fileNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, fileName.c_str(), fileNameLength);
		offset += fileNameLength;
	}
	
	
	// Is the file-type field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 4);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &userType, sizeof(int));
		offset += sizeof(int);
	}
	
    *tupleSize = offset;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attrs.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    return attrs;
}

void RelationManager::prepareColumnTuple(int attributeCount, unsigned char *nullAttributesIndicator, const int tableId, const int columnNameLength, 
    const string &columnName, const int columnType, const int columnLength, const int columnPosition, void *buffer, int *tupleSize)
{
    int offset = 0;

	// Null-indicators
    bool nullBit = false;
    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

	// Null-indicator for the fields
    memcpy((char *)buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
	offset += nullAttributesIndicatorActualSize;

	// Beginning of the actual data    
	// Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
	// e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

	// Is the table-id field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 7);

    if (!nullBit) {
		memcpy((char *)buffer + offset, &tableId, sizeof(int));
		offset += sizeof(int);
	}
	
	// Is the column-name field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 6);

    if (!nullBit) {
		memcpy((char *)buffer + offset, &columnNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy((char *)buffer + offset, columnName.c_str(), columnNameLength);
		offset += columnNameLength;
	}
	
	
	// Is the column-type field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 5);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &columnType, sizeof(int));
		offset += sizeof(int);
	}
	
	
	// Is the column-length field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 4);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &columnLength, sizeof(int));
		offset += sizeof(int);
	}

    // Is the column-position field not-NULL?
	nullBit = nullAttributesIndicator[0] & (1 << 3);

	if (!nullBit) {
		memcpy((char *)buffer + offset, &columnPosition, sizeof(int));
		offset += sizeof(int);
	}
	
    *tupleSize = offset;
}