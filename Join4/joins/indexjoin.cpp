#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/minirel.h"
#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/join.h"
#include "../include/btfile.h"
#include "../include/btfilescan.h"
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


void IndexNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, long& pinRequests, long& pinMisses, double& duration)
{

    
    {
        Status status;
        int innerRecLen = specOfS.recLen;
        HeapFile* innerFile = specOfS.file;
        int outerRecLen = specOfR.recLen;
        char *innerRecPtr = new char[innerRecLen], * outerRecPtr = new char[outerRecLen];
        RecordID outerRid, resultRid;
        Scan *innerScan, *outerScan;
        int innerOffset = specOfS.offset;
        int outerOffset = specOfR.offset;
        int outAttribute;
        char* newRec = new char[innerRecLen + outerRecLen]; //allocate space for new record

        HeapFile* result = new HeapFile("SR_Index", status);
        long init_pins, init_misses;
        auto start = chrono::steady_clock::now();
        MINIBASE_BM->GetStat(init_pins, init_misses);
        int key;

        
        innerScan = innerFile->OpenScan(status);
       
        if(status != OK)
            cerr << "Well this really sucks. We can't seem to open the scan on inner relation's heapfile.\n";

        
        /* build up BTree of the inner relation S */
        BTreeFile *btree;
        btree = new BTreeFile (status, "BTree", ATTR_INT, sizeof(int));
        
        RecordID rid;
        while (innerScan->GetNext(rid, innerRecPtr, innerRecLen) == OK)
            btree->Insert(innerRecPtr + innerOffset, rid);
        
        delete innerScan;
        
        //Open outer scan
        
        outerScan = specOfR.file->OpenScan(status);
        
        if(status != OK) cerr << "Well this really sucks. We can't seem to open the scan on outer relation's heapfile.\n";
            
        //For each of the outer tuples, scan B tree for matches
        while (outerScan->GetNext(outerRid, outerRecPtr, outerRecLen) == OK){
            //Identify the outer attribute
            memcpy(&outAttribute, outerRecPtr + outerOffset, sizeof(int));
            IndexFileScan* scan = btree->OpenSearchScan(&outAttribute, &outAttribute);
            
            while(scan->GetNext(rid, &key) == OK){
                innerFile->GetRecord(rid, innerRecPtr, innerRecLen);
                MakeNewRecord(newRec, outerRecPtr, innerRecPtr, outerRecLen, innerRecLen);
                result->InsertRecord(newRec, innerRecLen + outerRecLen, resultRid);
                
            }
            delete scan;
        }
        
            duration =  chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
            
            long pins, misses;
            
            MINIBASE_BM->GetStat(pins, misses);
        
        pinRequests = pins - init_pins; pinMisses = misses - init_misses;
        
        delete outerScan;
        delete[] innerRecPtr; delete[] outerRecPtr;
        delete result; delete btree;
    }

    
}
