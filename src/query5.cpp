#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <map>

static inline std::vector<std::string> split(const std::string &s, char delim='|') {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == delim) { out.push_back(cur); cur.clear(); }
        else cur.push_back(c);
    }
    out.push_back(cur);
    return out;
}

// -------------------- Parse Args --------------------
bool parseArgs(int argc, char* argv[],
               std::string& r_name,
               std::string& start_date,
               std::string& end_date,
               int& num_threads,
               std::string& table_path,
               std::string& result_path) {

    r_name = "ASIA";
    start_date = "1994-01-01";
    end_date = "1995-01-01";
    num_threads = 1;

    table_path.clear();
    result_path.clear();

    for (int i = 1; i < argc; i++) {
        std::string k = argv[i];
        auto need = [&](const std::string& key)->bool{
            if (i+1 >= argc) {
                std::cerr << "Missing value for " << key << "\n";
                return false;
            }
            return true;
        };

        if (k == "--r_name") { if(!need(k)) return false; r_name = argv[++i]; }
        else if (k == "--start_date") { if(!need(k)) return false; start_date = argv[++i]; }
        else if (k == "--end_date") { if(!need(k)) return false; end_date = argv[++i]; }
        else if (k == "--threads") { if(!need(k)) return false; num_threads = std::stoi(argv[++i]); }
        else if (k == "--table_path") { if(!need(k)) return false; table_path = argv[++i]; }
        else if (k == "--result_path") { if(!need(k)) return false; result_path = argv[++i]; }
        else {
            std::cerr << "Unknown arg: " << k << "\n";
            return false;
        }
    }

    if (num_threads <= 0) num_threads = 1;
    if (table_path.empty() || result_path.empty()) {
        std::cerr << "table_path and result_path are required!\n";
        return false;
    }
    return true;
}


bool readTPCHData(const std::string&,
                  std::vector<std::map<std::string, std::string>>&,
                  std::vector<std::map<std::string, std::string>>&,
                  std::vector<std::map<std::string, std::string>>&,
                  std::vector<std::map<std::string, std::string>>&,
                  std::vector<std::map<std::string, std::string>>&,
                  std::vector<std::map<std::string, std::string>>&) {
    return true;
}

// -------------------- Optimized Query5 --------------------
static long long fileSize(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    return (long long)f.tellg();
}

static void seekToNextLine(std::ifstream& fin) {
    std::string dummy;
    std::getline(fin, dummy);
}

bool executeQuery5(const std::string& r_name,
                   const std::string& start_date,
                   const std::string& end_date,
                   int num_threads,
                   const std::vector<std::map<std::string, std::string>>&,
                   const std::vector<std::map<std::string, std::string>>&,
                   const std::vector<std::map<std::string, std::string>>&,
                   const std::vector<std::map<std::string, std::string>>&,
                   const std::vector<std::map<std::string, std::string>>&,
                   const std::vector<std::map<std::string, std::string>>&,
                   std::map<std::string, double>& results) {

    results.clear();
    if (num_threads <= 0) num_threads = 1;

    std::string base = start_date; 
    (void)base;

    std::string tp = r_name;
    (void)tp;

    std::string p = "";
  

    std::ifstream tpf(".table_path_tmp");
    std::string table_path;
    if (tpf.is_open()) {
        std::getline(tpf, table_path);
        tpf.close();
    } else {
        std::cerr << "ERROR: .table_path_tmp not found. Create it with table_path.\n";
        return false;
    }

    if (!table_path.empty() && table_path.back() != '/') table_path += "/";

    std::string regionFile  = table_path + "region.tbl";
    std::string nationFile  = table_path + "nation.tbl";
    std::string customerFile= table_path + "customer.tbl";
    std::string supplierFile= table_path + "supplier.tbl";
    std::string ordersFile  = table_path + "orders.tbl";
    std::string lineitemFile= table_path + "lineitem.tbl";

    
    std::unordered_map<std::string,int> regionNameToKey;
    {
        std::ifstream fin(regionFile);
        if(!fin.is_open()){ std::cerr<<"Cannot open "<<regionFile<<"\n"; return false; }
        std::string line;
        while(std::getline(fin,line)){
            if(!line.empty() && line.back()=='|') line.pop_back();
            auto a = split(line);
            int rk = std::stoi(a[0]);
            std::string rn = a[1];
            regionNameToKey[rn]=rk;
        }
    }
    if(regionNameToKey.find(r_name)==regionNameToKey.end()){
        std::cerr<<"Region not found: "<<r_name<<"\n"; return false;
    }
    int targetRegionKey = regionNameToKey[r_name];

   
    std::unordered_map<int,std::string> nationKeyToName;
    std::unordered_map<int,int> nationKeyToRegion;
    {
        std::ifstream fin(nationFile);
        if(!fin.is_open()){ std::cerr<<"Cannot open "<<nationFile<<"\n"; return false; }
        std::string line;
        while(std::getline(fin,line)){
            if(!line.empty() && line.back()=='|') line.pop_back();
            auto a = split(line);
            int nk = std::stoi(a[0]);
            std::string nn = a[1];
            int rk = std::stoi(a[2]);
            nationKeyToName[nk]=nn;
            nationKeyToRegion[nk]=rk;
        }
    }

   
    std::unordered_map<int,int> custNation;
    {
        std::ifstream fin(customerFile);
        if(!fin.is_open()){ std::cerr<<"Cannot open "<<customerFile<<"\n"; return false; }
        std::string line;
        while(std::getline(fin,line)){
            if(!line.empty() && line.back()=='|') line.pop_back();
            auto a = split(line);
            int ck = std::stoi(a[0]);
            int nk = std::stoi(a[3]);
            custNation[ck]=nk;
        }
    }

  
    std::unordered_map<int,int> suppNation;
    {
        std::ifstream fin(supplierFile);
        if(!fin.is_open()){ std::cerr<<"Cannot open "<<supplierFile<<"\n"; return false; }
        std::string line;
        while(std::getline(fin,line)){
            if(!line.empty() && line.back()=='|') line.pop_back();
            auto a = split(line);
            int sk = std::stoi(a[0]);
            int nk = std::stoi(a[3]);
            suppNation[sk]=nk;
        }
    }

   
    std::unordered_map<int,int> validOrders; 
    {
        std::ifstream fin(ordersFile);
        if(!fin.is_open()){ std::cerr<<"Cannot open "<<ordersFile<<"\n"; return false; }
        std::string line;
        while(std::getline(fin,line)){
            if(!line.empty() && line.back()=='|') line.pop_back();
            auto a = split(line);
            int ok = std::stoi(a[0]);
            int ck = std::stoi(a[1]);
            std::string od = a[4];

            if(!(od >= start_date && od < end_date)) continue;

            auto itC = custNation.find(ck);
            if(itC==custNation.end()) continue;

            int nk = itC->second;
            if(nationKeyToRegion[nk] != targetRegionKey) continue;

            validOrders[ok]=ck;
        }
    }

    long long sz = fileSize(lineitemFile);
    long long chunk = (sz + num_threads - 1) / num_threads;

    std::mutex mergeMutex;

    auto worker = [&](int tid){
        long long start = tid * chunk;
        long long end   = std::min(sz, start + chunk);

        std::ifstream fin(lineitemFile);
        if(!fin.is_open()) return;

        fin.seekg(start, std::ios::beg);
        if(start != 0) seekToNextLine(fin); 

        std::unordered_map<std::string,double> localAgg;
        std::string line;

        while(true){
            auto pos = fin.tellg();
            if(pos == -1) break;
            if((long long)pos >= end) break;

            if(!std::getline(fin,line)) break;
            if(!line.empty() && line.back()=='|') line.pop_back();

            auto a = split(line);

            int orderkey = std::stoi(a[0]);
            int suppkey  = std::stoi(a[2]);
            double extp  = std::stod(a[5]);
            double disc  = std::stod(a[6]);

            auto itO = validOrders.find(orderkey);
            if(itO==validOrders.end()) continue;

            int custkey = itO->second;

            auto itCN = custNation.find(custkey);
            if(itCN==custNation.end()) continue;

            auto itSN = suppNation.find(suppkey);
            if(itSN==suppNation.end()) continue;

            if(itCN->second != itSN->second) continue; 

            int nk = itSN->second;
            const std::string &nname = nationKeyToName[nk];

            localAgg[nname] += extp * (1.0 - disc);
        }

        std::lock_guard<std::mutex> lk(mergeMutex);
        for(auto &p: localAgg) results[p.first] += p.second;
    };

    std::vector<std::thread> threads;
    for(int t=0;t<num_threads;t++) threads.emplace_back(worker,t);
    for(auto &th: threads) th.join();

    return true;
}

// -------------------- Output Results --------------------
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::string base = result_path;
    if(!base.empty() && base.back()!='/') base+="/";

    std::ofstream fout(base + "query5_result.txt");
    if(!fout.is_open()){
        std::cerr<<"Cannot write result file\n";
        return false;
    }

    std::vector<std::pair<std::string,double>> vec(results.begin(), results.end());
    std::sort(vec.begin(), vec.end(),
              [](const std::pair<std::string,double>& a,
                 const std::pair<std::string,double>& b){
                    if(a.second != b.second) return a.second > b.second;
                    return a.first < b.first;
              });

    fout << "n_name|revenue\n";
    for(auto &p: vec) fout << p.first << "|" << p.second << "\n";

    fout.close();

    std::cout << "===== TPCH Query 5 Result =====\n";
    for(auto &p: vec) std::cout << p.first << " : " << p.second << "\n";
    std::cout << "Saved to: " << base << "query5_result.txt\n";

    return true;
}
