#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
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

void SortMergeJoin (JoinSpec specOfR, JoinSpec specOfS, long& pinRequests, long& pinMisses, double& duration)
{

    int innerLen = specOfS.recLen;
    int outerLen = specOfR.recLen;
    char *innerRecPtr = new char[innerLen], * outerRecPtr = new char[outerLen];
    RecordID innerRid, outerRid, resultRid;
    Scan *innerScan, *outerScan;
    int innerOffset = specOfS.offset;
    int outerOffset = specOfR.offset;
    int outAttribute(INT_MIN), inAttribute(INT_MIN+1);
    Status outStatus(OK);  Status inStatus(OK);
    char* newRec = new char[innerLen + outerLen];
    HeapFile* result = new HeapFile("SR_Sort", inStatus);
    long init_pins, init_misses;
    auto start = chrono::steady_clock::now();
    
    HeapFile* SortedS = SortFile(specOfS.file, innerLen, innerOffset);
    
    HeapFile* SortedR = SortFile(specOfR.file, outerLen, outerOffset);


    MINIBASE_BM->GetStat(init_pins, init_misses);
    
    //Try to start scans for both files
    
    innerScan = SortedS->OpenScan(inStatus);

    if(inStatus != OK) cerr << "Well this really sucks. We can't seem to open the scan on inner relation's heapfile.\n";
    
    outerScan = SortedR->OpenScan(outStatus);
    
    if(outStatus != OK) cerr << "Well this really sucks. We can't seem to open the scan on outer relation's heapfile.\n";
    //For each of the outer tuples, scan entire innner relation for join matches
    
    vector<char*> innerRecPointers;
    char * recPtr;
    
    //load in attributes of interest
    

    
    do{
        
        
        if(outAttribute < inAttribute){ //Load in next tuple
            if(outerScan->GetNext(outerRid, outerRecPtr, outerLen) == OK)
                memcpy(&outAttribute, outerRecPtr + outerOffset, sizeof(int));
            else break; //No more matches possible
        }
        else if(outAttribute > inAttribute){ //Load in next tuple
            if(innerScan->GetNext(innerRid, innerRecPtr, innerLen) == OK)
                memcpy(&inAttribute, innerRecPtr + innerOffset, sizeof(int));
            else break; //No more matches possible
        }
        
        else{ //They are equal
            int oldAttribute = outAttribute;
            innerRecPointers.push_back(innerRecPtr);
            do{
                recPtr = new char[innerLen];
                inStatus = innerScan->GetNext(innerRid, recPtr, innerLen);
                if(inStatus == OK){
                    memcpy(&inAttribute, recPtr + innerOffset, sizeof(int));
                    if(inAttribute == outAttribute)
                        innerRecPointers.push_back(recPtr);
                }
                
            }while(inStatus == OK && inAttribute == oldAttribute);
            
            do{
                for(char * ptr : innerRecPointers){
                    MakeNewRecord(newRec, outerRecPtr, ptr, outerLen, innerLen);
                    result->InsertRecord(newRec, innerLen + outerLen, resultRid);
                }
                
                outStatus = outerScan->GetNext(outerRid, outerRecPtr, outerLen);
                
                if(outStatus == OK){
                    memcpy(&outAttribute, outerRecPtr + outerOffset, sizeof(int));
                }
                
                
            }while(outAttribute == oldAttribute && outStatus == OK);
            
            
            for(char* rec : innerRecPointers) delete[] rec;
            
            innerRecPtr = recPtr;
            
            innerRecPointers.clear();
            
            
        }
        
    }while(outStatus == OK && inStatus == OK);
    
    
    duration =  chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
    
    long pins, misses;
    
    MINIBASE_BM->GetStat(pins, misses);
    
    pinRequests = pins - init_pins; pinMisses = misses - init_misses;
    
    delete outerScan; delete[] newRec; delete innerScan;
    delete[] innerRecPtr; delete[] outerRecPtr;
    delete result;
    

    
}
