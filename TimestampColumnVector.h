//
// Created by liyu on 12/23/23.
//

#ifndef DUCKDB_TIMESTAMPCOLUMNVECTOR_H
#define DUCKDB_TIMESTAMPCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <string>
#include <chrono>
#include <ctime>
#include <stdexcept>
#include <cstdint>

class TimestampColumnVector: public ColumnVector {
public:
    int precision;
    long * times;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    explicit TimestampColumnVector(int precision, bool encoding = false);
    explicit TimestampColumnVector(uint64_t len, int precision, bool encoding = false);
    void * current() override;
    void set(int elementNum, long ts);
    ~TimestampColumnVector();
    void print(int rowCount) override;
    void close() override;
    void add(int64_t micros) override;
    void add(const std::chrono::system_clock::time_point& value) override;
    void add(const std::string& value) override; 
    void ensureSize(uint64_t size, bool preserveData) override;
private:
    bool isLong;
};
#endif //DUCKDB_TIMESTAMPCOLUMNVECTOR_H
