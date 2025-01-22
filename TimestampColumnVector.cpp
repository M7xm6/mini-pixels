//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    } else {
        this->times = nullptr;
    }
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    //throw InvalidArgumentException("not support print longcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
      std::cout<<longVector[i]<<std::endl;
	std::cout<<intVector[i]<<std::endl;
  }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}
void TimestampColumnVector::add(int64_t micros) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index= writeIndex++
    timestamps[index] = micros;
    isNull[index] = false;
    noNulls = true;
}

// ���һ��std::chrono::system_clock::time_point���͵�ʱ���
void TimestampColumnVector::add(const std::chrono::system_clock::time_point& value) {
    auto duration = value.time_since_epoch();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    add(micros); // �������΢��ķ���
}

// ���һ���ַ�����ʾ��ʱ�������Ҫת��Ϊ΢�룩
void TimestampColumnVector::add(const std::string& value) {
    int64_t micros = stringTimestampToMicros(value);
    add(micros); // �������΢��ķ���
}


void ensureSize(uint64_t size, bool preserveData) override {
    if (size > length) {
        int64_t* newTimestamps = new int64_t[size];
        uint8_t* newIsNull = new uint8_t[size];
        uint64_t* newIsValid = new uint64_t[(size / 64) + 1];

        if (preserveData) {
            std::copy(timestamps, timestamps + std::min(writeIndex, length), newTimestamps);
            std::copy(isNull, isNull + std::min(writeIndex, length), newIsNull);
            // ���� isValid ����
            std::copy(isValid, isValid + ((length / 64) + 1), newIsValid);
            // ����´�С����64�ı�������Ҫ�ر������һ����������64λ��
            if (length % 64 != 0) {
                uint64_t mask = (1ULL << (length % 64)) - 1;
                newIsValid[size / 64] &= mask; // ֻ������Чλ
            }
            // ��ʼ������ӵ�Ԫ�ص� isValid λΪ��Ч
            std::fill(newIsValid + ((length / 64) + 1), newIsValid + ((size / 64) + 1), ~0ULL);
        }
        else {
            // ������������ݣ����ʼ��������
            std::fill(newIsValid, newIsValid + ((size / 64) + 1), 0ULL); 
        }

        delete[] timestamps;
        delete[] isNull;
        delete[] isValid;

        timestamps = newTimestamps;
        isNull = newIsNull;
        isValid = newIsValid;

        length = size;
    }
}


// �������������ַ���ʱ���ת��Ϊ΢��
int64_t stringTimestampToMicros(const std::string& value) {
    // ����ʱ��ṹ��
    std::tm tm = {};
    // ����΢�벿��
    std::chrono::microseconds us;

    // �����ַ�����
    std::istringstream ss(value);
    // ʹ�� get_time ����ʱ�䲿��
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    // ����Ƿ�ɹ���������
    if (ss.fail() && !(ss.eofbit & ss.rdstate())) {
        throw std::invalid_argument("Failed to parse date and time part of the timestamp");
    }

    // ��������־
    ss.clear();

    // ����΢�벿��
    char microsStr[7]; // ΢�벿�����6λ����+'\0'
    if (ss.read(microsStr, 6).gcount() == 6) {
        // ��΢�벿��ת��Ϊ����
        int microsInt = std::stoi(microsStr);
        // ת��Ϊchrono::microseconds
        us = std::chrono::microseconds(microsInt);
    }
    else {
        // ���û��΢�벿�֣���Ĭ��Ϊ0
        us = std::chrono::microseconds(0);
    }

    // ��tmת��Ϊtime_t����1970-01-01 00:00:00 UTC�����ڵ�������
    std::time_t timeSinceEpoch = std::mktime(&tm);
    if (timeSinceEpoch == -1) {
        throw std::runtime_error("Failed to convert tm to time_t");
    }

    // ����ת��Ϊchrono::seconds
    auto seconds = std::chrono::seconds(timeSinceEpoch);

    // �����΢����ϳ��ܵ�duration
    auto duration = seconds + us;

    // ��durationת��Ϊ΢��
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}
