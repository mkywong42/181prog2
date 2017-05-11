#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;
RecordBasedFileManager *RBFM_ScanIterator::_rbf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RBFM_ScanIterator::RBFM_ScanIterator()
{
    _rbf_manager = RecordBasedFileManager::instance();
    currentPage = 0;
    currentSlot = 0;
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void* data)
{      
    
//     RID rid;
//     void* attribute = malloc(1000);
//     unsigned i;
//     for(unsigned k = 0; k<totalPages;k++){
//         //get the page
//         void * pageData = malloc(PAGE_SIZE);
//         if (fileHandle.readPage(k, pageData))
//             return RBFM_READ_FAILED;

//         SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
//         for(unsigned j = 0; j<slotHeader.recordEntriesNumber;j++){          //need to check if dead
//             SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, j);
//             if(recordEntry.length == 0) continue;
//             for(i = 0; i<recordDescriptor.size(); i++){
//                 if(recordDescriptor[i].name == conditionAttribute){
//                     break;
//                 }   
//             }
//             rid.pageNum = k;
//             rid.slotNum = j;
//             if(readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, attribute))
//                 return RBFM_READ_ATTRIBUTE_FAIL;

//             int result = comparison(attribute, value, recordDescriptor[i]);

//             switch(compOp){
//                 case EQ_OP:
//                     if(result == 0 ){
//                         //insert
//                     }
//                     break;
//                 case LT_OP:
//                     if(result == -1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case LE_OP:
//                     if(result == 0 || result == -1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case GT_OP:
//                     if(result == 1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case GE_OP:
//                     if(result == 0 || result == 1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case NE_OP:
//                     if(result != 0){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case NO_OP:
//                     rbfm_ScanIterator.ridResult.push_back(rid);
//                     break;
//             }
//             memset(attribute, 0, 1000);
//         }
//         free(pageData);
//     }
//     return SUCCESS;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);
    if(recordSize > PAGE_SIZE - sizeof(SlotDirectoryHeader)-sizeof(SlotDirectoryRecordEntry)){
        return RBFM_INVALID_RECORD_SIZE;
    }

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

//added
    if(recordEntry.length == 0){

        free(pageData);
        return RBFM_RECORD_IS_DEAD;
    }else if( recordEntry.offset <0 ){
        int tempPage = (-1)* recordEntry.offset;
        int tempSlot = recordEntry.length;
        RID temp;
        temp.pageNum = tempPage;
        temp.slotNum = tempSlot;
        readRecord(fileHandle,recordDescriptor, temp, data);
    }else{
        // Retrieve the actual entry data
        getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

// Private helper methods

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memmove (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memmove  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    // Retrieve the specific page

    // 1.  get the page
    // 2.  find the slot directory
    //     a.  validate
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // 3.  get the slot directory header
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // Retrieve the actual entry data
    //getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    // 4.  if dead then do nothing
    if(recordEntry.length == 0){
        free(pageData);
        return 0;
        
    // 5.  if moved then
    }else if(recordEntry.offset < 0){
	//     a. detected
        int new_pageId = (-1)*recordEntry.offset;
        int new_slotId = recordEntry.length;
        RID temp;
        temp.pageNum = new_pageId;
        temp.slotNum = new_slotId;
    //     b. repeat delete (recursion)
        deleteRecord(fileHandle, recordDescriptor, temp);
	//     c. compaction

    // 6.  if alive then
    }else{
	//     b. compaction
        compaction(fileHandle, rid, recordEntry.length, pageData);
        if (fileHandle.writePage(rid.pageNum, pageData))
            return RBFM_WRITE_FAILED;
    }

    free(pageData);
    return 0;

}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
//      1.  find the page
//      2.  find the slot directory
// 	        a. validate

    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    //3.  get the slot directory entry data
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);   
// 	        A. if dead then throw error
    if(recordEntry.length == 0){
        free(pageData);
        return RBFM_RECORD_IS_DEAD;

    //b. if moved then repeat(recursion)
    }else if(recordEntry.offset < 0){
        int new_pageId = (-1)*recordEntry.offset;
        int new_slotId = recordEntry.length;
        RID temp;
        temp.pageNum = new_pageId;
        temp.slotNum = new_slotId;
        updateRecord(fileHandle, recordDescriptor, data, temp);

    //4.  if alive then
    }else{
        unsigned newRecordSize = getRecordSize(recordDescriptor, data);
        unsigned oldRecordSize = recordEntry.length;

        // a. new record same size
        if(newRecordSize==oldRecordSize){
            // -replace
            setRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

            // b. new record smaller
        }else if(newRecordSize < oldRecordSize){
// 		        -shrinking
            unsigned newOffset = compaction(fileHandle, rid, oldRecordSize - newRecordSize, pageData);
            setRecordAtOffset(pageData, newOffset, recordDescriptor, data);
            recordEntry.offset = newOffset;
            recordEntry.length = newRecordSize;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
      
// 	        c. new record bigger
        }else{
// 		    -have enough free space
            unsigned freePageSpace = getPageFreeSpaceSize(pageData);
            if(freePageSpace >= newRecordSize){
// 			        1. expand
                unsigned newOffset = compaction(fileHandle, rid, oldRecordSize-newRecordSize, pageData);
//                  2. insert
                setRecordAtOffset(pageData, newOffset, recordDescriptor, data);

// 		    - don’t have enough free space
            }else{
// 			    1. find page
// 			    2. insert record
                deleteRecord(fileHandle, recordDescriptor, rid);
                RID temp;
                insertRecord(fileHandle, recordDescriptor, data, temp);

//              3. update forwarding address
                recordEntry.offset = temp.pageNum * -1;
                recordEntry.length = temp.slotNum;
                setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
            }
        }
        if (fileHandle.writePage(rid.pageNum, pageData))
            return RBFM_WRITE_FAILED;
    }
       
    return 0;

}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    //get the page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    void* rData;

    if(recordEntry.length == 0){
        free(pageData);
        return 0;
    }else if( recordEntry.offset <0 ){
        int tempPage = (-1)* recordEntry.offset;
        int tempSlot = recordEntry.length;
        RID temp;
        temp.pageNum = tempPage;
        temp.slotNum = tempSlot;
        readRecord(fileHandle,recordDescriptor, temp, rData);
    }else{
        // Retrieve the actual entry data
        rData = malloc(recordEntry.length);
        getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, rData);
    }

    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*)rData, nullIndicatorSize);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    //find index of attribute and checks to see if attribute is null
    //if attribute is null then break with error
    unsigned i;
    for(i=0; i<recordDescriptor.size();i++){
        if(recordDescriptor[i].name == attributeName){
            break;
        }else{
            bool isNull = fieldIsNull(nullIndicator, i);
            if(!isNull){
                if(recordDescriptor[i].type == TypeVarChar){
                    unsigned varCharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varCharSize, (char*)rData+offset, VARCHAR_LENGTH_SIZE);
                    offset+=varCharSize + VARCHAR_LENGTH_SIZE;
                }else{
                    offset+=recordDescriptor[i].length;
                }
            }
        }
    }
    string nullString = "NULL";                    //might be wrong==================================================
    if(fieldIsNull(nullIndicator, i)){
        memcpy((char*)data, &nullString, 4);
        free(pageData);
        free(rData);
        return 0;
    }
    //if record is a VarChar then get length of VarChar then read VarChar into data
    if(recordDescriptor[i].type == TypeVarChar){
        unsigned varCharSize;
        // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
        memcpy(&varCharSize, (char*)rData+offset, VARCHAR_LENGTH_SIZE);
        offset+=varCharSize;
        memcpy((char*)data, (char*)rData+offset, varCharSize);
    }else{      //if record is not a VarChar then copy value into data
        memcpy((char*)data, (char*)rData+offset,recordDescriptor[i].length);
    }

    free(pageData);
    free(rData);
    return 0;
}


RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
      const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{
    rbfm_ScanIterator.fileHandle = fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;            //wrong========================================
    rbfm_ScanIterator.attributeNames = attributeNames;
    rbfm_ScanIterator.totalPages = fileHandle.getNumberOfPages();
    return SUCCESS;
//     RID rid;
//     void* attribute = malloc(1000);
//     unsigned i;
//     for(unsigned k = 0; k<fileHandle.getNumberOfPages();k++){
//         //get the page
//         void * pageData = malloc(PAGE_SIZE);
//         if (fileHandle.readPage(k, pageData))
//             return RBFM_READ_FAILED;

//         SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
//         for(unsigned j = 0; j<slotHeader.recordEntriesNumber;j++){          //need to check if dead
//             SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, j);
//             if(recordEntry.length == 0) continue;
//             for(i = 0; i<recordDescriptor.size(); i++){
//                 if(recordDescriptor[i].name == conditionAttribute){
//                     break;
//                 }   
//             }
//             rid.pageNum = k;
//             rid.slotNum = j;
//             if(readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, attribute))
//                 return RBFM_READ_ATTRIBUTE_FAIL;

//             int result = comparison(attribute, value, recordDescriptor[i]);

//             switch(compOp){
//                 case EQ_OP:
//                     if(result == 0 ){
//                         //insert
//                     }
//                     break;
//                 case LT_OP:
//                     if(result == -1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case LE_OP:
//                     if(result == 0 || result == -1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case GT_OP:
//                     if(result == 1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case GE_OP:
//                     if(result == 0 || result == 1){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case NE_OP:
//                     if(result != 0){
//                         rbfm_ScanIterator.ridResult.push_back(rid);
//                     }
//                     break;
//                 case NO_OP:
//                     rbfm_ScanIterator.ridResult.push_back(rid);
//                     break;
//             }
//             memset(attribute, 0, 1000);
//         }
//         free(pageData);
//     }
//     return SUCCESS;
}

int RecordBasedFileManager::comparison(const void* attribute, const void* value, Attribute attr)
{
    if(attr.type == TypeVarChar){
        unsigned lengthVarChar;
        memcpy(&lengthVarChar, attribute, sizeof(int));
        return memcmp((char*)attribute + sizeof(int), value, lengthVarChar);
    }else if( attr.type == TypeInt){
        if(*((int*)attribute) == *((int*)value)) return 0;
        else if(*((int*)attribute) < *((int*)value)) return -1;
        else if(*((int*)attribute) > *((int*)value)) return 1;
    }else{
        if(*((float*)attribute) == *((float*)value)) return 0;
        else if(*((float*)attribute) < *((float*)value)) return -1;
        else if(*((float*)attribute) > *((float*)value)) return 1;
    }
    return RBFM_COMPARE_FAIL;

}

unsigned RecordBasedFileManager::compaction(FileHandle &fileHandle, const RID &rid, const int change, void* pageData)
{
    // void * pageData = malloc(PAGE_SIZE);
    // if (fileHandle.readPage(rid.pageNum, pageData))
    //     return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotDirectoryHeader = getSlotDirectoryHeader(pageData);
    if(slotDirectoryHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    unsigned movingBlockSize = recordEntry.offset - slotDirectoryHeader.freeSpaceOffset;
    unsigned newOffset = recordEntry.offset + change;
    memmove((char*)pageData+slotDirectoryHeader.freeSpaceOffset+change,
        (char*)pageData+slotDirectoryHeader.freeSpaceOffset,movingBlockSize);
    for(int i = rid.slotNum+1; i<slotDirectoryHeader.recordEntriesNumber;i++){
        SlotDirectoryRecordEntry temp = getSlotDirectoryRecordEntry(pageData, i);
        temp.offset += change;
        setSlotDirectoryRecordEntry(pageData, i, temp);
    }
    recordEntry.length -= change;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

    slotDirectoryHeader.freeSpaceOffset+=change;
    setSlotDirectoryHeader(pageData, slotDirectoryHeader);

    return newOffset;
}