//
// Created by yuly on 05.04.23.
//

#ifndef PIXELS_DECIMALCOLUMNVECTOR_H
#define PIXELS_DECIMALCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class DecimalColumnVector: public ColumnVector {
public:
    long * vector;
    int precision;
    int scale;
    static long DEFAULT_UNSCALED_VALUE;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    DecimalColumnVector(int precision, int scale, bool encoding = false);
    DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding = false);
    ~DecimalColumnVector();
    void print(int rowCount) override;
    void close() override;
    void * current() override;
	int getPrecision();
	int getScale();
    void add(const string& value) override;
    void add(int value) override;
    void add(float value) override;
    void add(double value) override;
    void ensureSize(size_t newSize, bool preserveData) override;
};

#endif //PIXELS_DECIMALCOLUMNVECTOR_H
