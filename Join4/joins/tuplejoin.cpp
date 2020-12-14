#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/minirel.h"
#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/join.h"
#include "../include/relation.h"
#include "../include/bufmgr.h"


//---------------------------------------------------------------
// Each join method takes in at least two parameters :
// - specOfS
// - specOfR
//
// They specify which relations we are going to join, which
// attributes we are going to join on, the offsets of the
// attributes etc.  specOfS specifies the inner relation while
// specOfR specifies the outer one.
//
//You can use MakeNewRecord() to create the new result record.
//
// Remember to clean up before exiting by "delete"ing any pointers
// that you "new"ed.  This includes any Scan/BTreeFileScan that
// you have opened.
//---------------------------------------------------------------


void TupleNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, long& pinRequests, long& pinMisses, double& duration)
{
    Status status;
    int innerRecLen = specOfS.recLen;
    int outerRecLen = specOfR.recLen;
    char *innerRecPtr = new char[innerRecLen], * outerRecPtr = new char[outerRecLen];
    RecordID innerRid, outerRid, resultRid;
    Scan *innerScan, *outerScan;
    int innerOffset = specOfS.offset;
    int outerOffset = specOfR.offset;
    int outAttribute, inAttribute;
    char* newRec = new char[innerRecLen + outerRecLen];
    HeapFile* result = new HeapFile("SXR", status);
    long init_pins, init_misses;
    auto start = chrono::steady_clock::now();
    
    MINIBASE_BM->GetStat(init_pins, init_misses);
    
    //Try to start scans for both files
    
    if(status != OK) cerr << "Well this really sucks. We can't seem to open the scan on inner relation's heapfile.\n";
    
    outerScan = specOfR.file->OpenScan(status);
    
    if(status != OK) cerr << "Well this really sucks. We can't seem to open the scan on outer relation's heapfile.\n";
    //For each of the outer tuples, scan entire innner relation for join matches
    while (outerScan->GetNext(outerRid, outerRecPtr, outerRecLen) == OK){
        memcpy(&outAttribute, outerRecPtr + outerOffset, sizeof(int));
        innerScan = specOfS.file->OpenScan(status); //Reset inner relation scan each time
        
        while(innerScan->GetNext(innerRid, innerRecPtr,innerRecLen) == OK){
            
            memcpy(&inAttribute, innerRecPtr + innerOffset, sizeof(int));
            if(!memcmp(&outAttribute, innerRecPtr + innerOffset, sizeof(int))){ //Compare attributes of join for a match
                MakeNewRecord(newRec, outerRecPtr, innerRecPtr, outerRecLen, innerRecLen);
                result->InsertRecord(newRec, innerRecLen + outerRecLen, resultRid);
                
            }
        } delete innerScan;
        
    }
    duration =  chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
    
    long pins, misses;
    
    MINIBASE_BM->GetStat(pins, misses);
    
    pinRequests = pins - init_pins; pinMisses = misses - init_misses;
        
    delete outerScan; delete[] newRec;
    delete[] innerRecPtr; delete[] outerRecPtr;
    delete result;
    
}






