//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding): ColumnVector(len, encoding) {
	// decimal column vector has no encoding so we don't allocate memory to this->vector
	this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;
    memoryUsage += (uint64_t) sizeof(uint64_t) * len;
}

void DecimalColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
		vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}



void DecimalColumnVector::ensureSize(size_t newSize, bool preserveData) {
    if (newSize > capacity) {
        // �������ڴ��С
        size_t newSizeInBytes = newSize * sizeof(Decimal);

        // ��ʱ�����ָ��
        Decimal* oldVector = vector;

        // ʹ�� posix_memalign �������ڴ棬���뵽32�ֽ�
        Decimal* newVector = nullptr;
        if (posix_memalign(reinterpret_cast<void**>(&newVector), 32, newSizeInBytes) != 0) {
            throw std::bad_alloc(); // �ڴ����ʧ��ʱ�׳��쳣
        }

        // �����Ҫ�������ݣ����ƾ����ݵ����ڴ�
        if (preserveData) {
            std::copy(oldVector, oldVector + std::min(length, newSize), newVector);
        }

        // ��ʼ�����ڴ���δ���ƵĲ���
        for (size_t i = length; i < newSize; ++i) {
            newVector[i].integerPart = 0;
            newVector[i].fractionalPart = 0;
        }
        // �ͷž��ڴ�
        free(oldVector);

        // ����ָ�������
        vector = newVector;
        capacity = newSize;

        memoryUsage = newSizeInBytes; 
    }
}


void DecimalColumnVector::add(const string& value){
    if (writeIndex >= getLength()){
        ensureSize(writeIndex * 2, true);
    }

    // Using cpp_dec_float for BigDecimal-like behavior with unlimited precision
    cpp_dec_float_50 decimal(value);

    // Adjust scale
    long double factor = pow(10.0L, scale);
    decimal *= factor;
    decimal = round(decimal * 0.5L) / 0.5L;  // ROUND_HALF_UP equivalent
    decimal /= factor;

    if (decimal.precision() > precision){
        throw illegal_argument("value exceeds the allowed precision " + to_string(precision));
    }
    int index = writeIndex++;
    // Convert unscaled value to long (assuming precision limit allows this)
    long long unscaledValue = llround(decimal.convert_to<long double>());
    vector[index] = unscaledValue;
    isNull[index] = false;
}

void DecimalColumnVector::add(float value){
    add(static_cast<double>(value));
}

void DecimalColumnVector::add(double value){
    if (writeIndex >= getLength())
    {
        ensureSize(writeIndex * 2, true);
    }

    cpp_dec_float_50 decimal(value);
    long double factor = pow(10.0L, scale);
    decimal *= factor;
    decimal = round(decimal); // ��Ϊ��׼�������룬������Ҫ�����������
    decimal /= factor;

    // ���ȼ�飬������Ҫ����ʵ�� precision �Ķ���������
    if (decimal.str().size() - decimal.str().find('.') - 1 > precision)
    {
        throw illegal_argument("value exceeds the allowed precision " + to_string(precision));
    }

    int index = writeIndex++;
    long long unscaledValue = llround(decimal.convert_to<long double>());
    vector[index] = unscaledValue;
    isNull[index] = false;
}

int DecimalColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    vector[index] = value;
    isNull[index] = false;
}
