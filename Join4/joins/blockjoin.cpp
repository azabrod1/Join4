#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unordered_map>
#include <unordered_set>

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

void BlockNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, int B, long& pinRequests, long& pinMisses, double& duration)
{
    Status status;
    int innerLen = specOfS.recLen;
    int outerLen = specOfR.recLen;
    char *innerRecPtr = new char[innerLen], * outerRecPtr = new char[outerLen];
    RecordID innerRid, outerRid, resultRid;
    Scan *innerScan, *outerScan;
    int innerOffset = specOfS.offset;
    int outerOffset = specOfR.offset;
    int outAttribute, inAttribute;
    char* newRec = new char[innerLen + outerLen];
    HeapFile* result = new HeapFile("SR_Block", status);
    long init_pins = 0, init_misses = 0;
    unordered_map<int,unordered_set<int>*> map;
    unordered_set<int>* tuples;
    char* block =  0;
    bool done  = false;
    auto start = chrono::steady_clock::now(); //Start the clock!

    MINIBASE_BM->GetStat(init_pins, init_misses);
    
    //Try to start scans for both files
    
    if(status != OK) cerr << "Well this really sucks. We can't seem to open the scan on inner relation's heapfile.\n";
    
    outerScan = specOfR.file->OpenScan(status);
    
    if(status != OK) cerr << "Well this really sucks. We can't seem to open the scan on outer relation's heapfile.\n";
    
    block = (char*) malloc(B);
    

    
    //For each of the outer tuples, scan entire innner relation for join matches
    while (!done){
        for(int offset = 0; offset < B - outerLen; offset += outerLen){
            if(outerScan->GetNext(outerRid, outerRecPtr, outerLen) != OK){
                done = true;
                break; //If we ran out of records to scan, nothing more to add to the block
            }
            
            memcpy(block+offset, outerRecPtr, outerLen); //Copy the record to block
            
            memcpy(&outAttribute, outerRecPtr + outerOffset, sizeof(int)); //get the join attribute
            
            if(!map.count(outAttribute))
                map[outAttribute] = new unordered_set<int>();
            
            map[outAttribute]->insert(offset); //This allows us to later find all tuples in the relation with a certain attribute value
            
        }
        
        innerScan = specOfS.file->OpenScan(status); //Reset inner relation scan each time
        
        while(innerScan->GetNext(innerRid, innerRecPtr,innerLen) == OK){ //Now iterate through inner relation
            
            memcpy(&inAttribute, innerRecPtr + innerOffset, sizeof(int)); //Find the value of tuple's join attribute
            
            if(map.count(inAttribute)){//Do we have any matching tuples in R?
                
                tuples = map[inAttribute];
                for (const auto& offset: *tuples) { //Add each matching tuple into the result relation
                    MakeNewRecord(newRec, block + offset, innerRecPtr, outerLen, innerLen);
                    result->InsertRecord(newRec, innerLen + outerLen, resultRid);
                    
                }
            }
            
        }
        
        delete innerScan; //Each time we iterate over the inner relation, we need to reset the inner scan
        
        // Iterator pointing to first element in map
        unordered_map<int,unordered_set<int>*>::iterator it = map.begin();
        
        // Erase all elements in the map we created
        while (it != map.end()) {
            // erase() function returns the iterator of the next element to last deleted element.
            delete it->second;
            it = map.erase(it);
        }
        map.clear();
        
    }
    
    duration =  chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
    
    long pins = 0,misses = 0;
    
    MINIBASE_BM->GetStat(pins, misses);
    
    pinRequests = pins - init_pins; pinMisses = misses - init_misses;
    
    delete outerScan;
    delete[] innerRecPtr; delete[] outerRecPtr;
    delete result; free(block);
    delete[] newRec;


    
}
