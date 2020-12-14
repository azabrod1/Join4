#include <stdlib.h>
#include <time.h>
#include <ctype.h>

#include "include/minirel.h"
#include "include/bufmgr.h"
#include "include/heapfile.h"
#include "include/join.h"
#include "include/relation.h"
#include <unordered_map>

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 8000 // define Buf manager size.You will need to change this for the analysis

unordered_map<int, int> buildMap(string filename, string heapName);
bool compareJoins(unordered_map<int, int> j1, unordered_map<int, int> j2);

int main()
{
    Status s;
    
    //
    // Initialize Minibase's Global Variables
    //
    
    minibase_globals = new SystemDefs(s,
                                      "MINIBASE.DB",
                                      "MINIBASE.LOG",
                                      NUM_OF_DB_PAGES,   // Number of pages allocated for database
                                      500,
                                      NUM_OF_BUF_PAGES,  // Number of frames in buffer pool
                                      NULL);
    
    //
    // Initialize random seed
    //
    
    srand(1);

    
    
    cerr << "Creating random records for relation R\n";
    CreateR();
    cerr << "Creating random records for relation S\n";
    CreateS();
    
    //
    // Initialize the specification for joins
    //
    
    JoinSpec specOfS, specOfR;
    
    CreateSpecForR(specOfR);
    CreateSpecForS(specOfS);
    
    cout << endl;
    
    
    long pinRequests, pinMisses;
    double duration;
    
    
    int blockSize = (MINIBASE_BM->GetNumOfUnpinnedFrames()-3*3)*MINIBASE_PAGESIZE;
    
    
    TupleNestedLoopJoin(specOfS, specOfR, pinRequests, pinMisses, duration);
    
    cout << "Tuple Nested Loop Join Duration: " << duration << endl;
    
    //MINIBASE_BM->FlushAllPages();
    
    BlockNestedLoopJoin(specOfS, specOfR,  10* MINIBASE_PAGESIZE, pinRequests, pinMisses, duration);
    
    cout << "10 Page Block Join Duration:      " << duration  <<endl;
    
   // MINIBASE_BM->FlushAllPages();
    
    
    IndexNestedLoopJoin(specOfS, specOfR, pinRequests, pinMisses, duration);
    
    cout << "Index loop join Duration:      " << duration  <<endl;
    
     SortMergeJoin(specOfS, specOfR, pinRequests, pinMisses, duration);
    
    cout << "Sort Merge join Duration:      " << duration  <<endl;
    

    unordered_map<int, int> blockJoin = buildMap("/Users/alex/Desktop/SR_Block.txt", "SR_Block");
    
    unordered_map<int, int> tupleJoin = buildMap("/Users/alex/Desktop/SR_Tuple.txt", "SXR");
    
    unordered_map<int, int> indexJoin = buildMap("/Users/alex/Desktop/SR_Index.txt", "SR_Index");
    
    unordered_map<int, int> sortJoin = buildMap("/Users/alex/Desktop/SR_Sort.txt", "SR_Sort");

    
    if(compareJoins(blockJoin, tupleJoin))
        cout << "Block Join and Tuple join give the same result" << endl;
    else
        cerr <<"Block Join and Tuple join give different results!" <<endl;
    
    
    if(compareJoins(blockJoin, indexJoin))
        cout << "Block Join and Index join give the same result" << endl;
    else
        cerr <<"Block Join and Index join give different results!" <<endl;
    
    
    //delete the created database
    remove("MINIBASE.DB");
    
    delete specOfS.file; delete specOfR.file;
    
    return 0;
}

/*Returns true if the two join results are equal */
bool compareJoins(unordered_map<int, int> j1, unordered_map<int, int> j2){
    
    if(j1.size() != j2.size()) return false;
    
    //Make sure that the results to tuple and block join match
    for(auto it = j1.begin(); it != j1.end(); ++it)
        if(j1.count(it->first) != j2.count(it->first)){
            cerr << "The result of nested loops join and block join does not match" << endl;
            return false;
        }
    return true;
}

unordered_map<int, int> buildMap(string filename, string heapName){
    Status s;
    HeapFile *SR = new HeapFile (heapName.c_str(), s); // new HeapFile storing records of R
    
    
    PrintResult(SR, (char*)filename.c_str());
    int input;
    //Opens for reading the file
    ifstream SR_file ( filename);
    //Reads one string from the file
    
    unordered_map<int, int> map;
    
    SR_file >> input;
    
    
    while (!SR_file.eof( ))      //if not at end of file, continue reading numbers
    {
        if(!map.count(input))
            map[input] = 1;
        else map[input]++;
        
        for(int i = 0; i < 8; ++i)
            SR_file >> input;
        
    }
    
    SR_file.close();
    
    delete SR;
    
    return map;
    
}


