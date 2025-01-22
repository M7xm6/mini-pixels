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

// 添加一个std::chrono::system_clock::time_point类型的时间戳
void TimestampColumnVector::add(const std::chrono::system_clock::time_point& value) {
    auto duration = value.time_since_epoch();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    add(micros); // 调用添加微秒的方法
}

// 添加一个字符串表示的时间戳（需要转换为微秒）
void TimestampColumnVector::add(const std::string& value) {
    int64_t micros = stringTimestampToMicros(value);
    add(micros); // 调用添加微秒的方法
}


void ensureSize(uint64_t size, bool preserveData) override {
    if (size > length) {
        int64_t* newTimestamps = new int64_t[size];
        uint8_t* newIsNull = new uint8_t[size];
        uint64_t* newIsValid = new uint64_t[(size / 64) + 1];

        if (preserveData) {
            std::copy(timestamps, timestamps + std::min(writeIndex, length), newTimestamps);
            std::copy(isNull, isNull + std::min(writeIndex, length), newIsNull);
            // 复制 isValid 数组
            std::copy(isValid, isValid + ((length / 64) + 1), newIsValid);
            // 如果新大小不是64的倍数，需要特别处理最后一个不完整的64位块
            if (length % 64 != 0) {
                uint64_t mask = (1ULL << (length % 64)) - 1;
                newIsValid[size / 64] &= mask; // 只保留有效位
            }
            // 初始化新添加的元素的 isValid 位为有效
            std::fill(newIsValid + ((length / 64) + 1), newIsValid + ((size / 64) + 1), ~0ULL);
        }
        else {
            // 如果不保留数据，则初始化新数组
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


// 辅助函数：将字符串时间戳转换为微秒
int64_t stringTimestampToMicros(const std::string& value) {
    // 定义时间结构体
    std::tm tm = {};
    // 定义微秒部分
    std::chrono::microseconds us;

    // 输入字符串流
    std::istringstream ss(value);
    // 使用 get_time 解析时间部分
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    // 检查是否成功解析到秒
    if (ss.fail() && !(ss.eofbit & ss.rdstate())) {
        throw std::invalid_argument("Failed to parse date and time part of the timestamp");
    }

    // 清除错误标志
    ss.clear();

    // 解析微秒部分
    char microsStr[7]; // 微秒部分最多6位数字+'\0'
    if (ss.read(microsStr, 6).gcount() == 6) {
        // 将微秒部分转换为整数
        int microsInt = std::stoi(microsStr);
        // 转换为chrono::microseconds
        us = std::chrono::microseconds(microsInt);
    }
    else {
        // 如果没有微秒部分，则默认为0
        us = std::chrono::microseconds(0);
    }

    // 将tm转换为time_t（从1970-01-01 00:00:00 UTC到现在的秒数）
    std::time_t timeSinceEpoch = std::mktime(&tm);
    if (timeSinceEpoch == -1) {
        throw std::runtime_error("Failed to convert tm to time_t");
    }

    // 将秒转换为chrono::seconds
    auto seconds = std::chrono::seconds(timeSinceEpoch);

    // 将秒和微秒组合成总的duration
    auto duration = seconds + us;

    // 将duration转换为微秒
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}
