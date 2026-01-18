#include "query5.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>

// TODO: Include additional headers as needed

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;

    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }

    std::vector<std::map<std::string, std::string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;

    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        std::cerr << "Failed to read TPCH data." << std::endl;
        return 1;
    }

     // For optimized query5.cpp (table_path workaround)
    {
        std::ofstream tmp(".table_path_tmp");
        tmp << table_path;
        tmp.close();
    }

    std::map<std::string, double> results;
     // START TIMER
    auto start = std::chrono::high_resolution_clock::now();
    
    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        std::cerr << "Failed to execute TPCH Query 5." << std::endl;
        return 1;
    }

    if (!outputResults(result_path, results)) {
        std::cerr << "Failed to output results." << std::endl;
        return 1;
    }

    
    // END TIMER 
    auto end = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(end - start).count();
    std::cout << "Query Execution Time: " << sec << " s" << std::endl;

    return 0;
} 
