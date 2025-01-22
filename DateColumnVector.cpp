//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} else {
		this->dates = nullptr;
	}
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    dates[writeIndex] = value;
    isNull[writeIndex++] = false;
}

void DateColumnVector::add(const std::string& value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    dates[writeIndex] = stringDateToDay(value);
    isNull[writeIndex++] = false;
}



void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        int* oldDates = dates;
        posix_memalign(reinterpret_cast<void**>(&dates), 32, size * sizeof(int32_t));
        if (preserveData) {
            std::copy(oldDates, oldDates + length, dates);
        }
        free(oldDates);
        memoryUsage += (long)sizeof(int) * (size - length);
        bool* oldIsNull = isNull;
        isNull = new bool[size](); // 新数组初始化为 false
        if (preserveData) {
            std::copy(oldIsNull, oldIsNull + length, isNull);
        }
        delete[] oldIsNull;
        resize(size);
    }
}

int DateColumnVector::stringDateToDay(const std::string& date_string) {
    std::tm tm = {}; // 初始化tm结构为0
    std::istringstream ss(date_string);
    char delimiter;

    // 解析日期字符串
    if (!(ss >> std::get_time(&tm, "%Y-%m-%d") && ss.eof())) {
        throw std::invalid_argument("日期格式错误: " + date_string + ". 正确格式为YYYY-MM-DD.");
    }

    // 设置tm_hour, tm_min, 和 tm_sec为0，表示午夜
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    // 设置tm_isdst为-1，让系统自动处理夏令时
    tm.tm_isdst = -1;

    // 将tm结构转换为time_t（自Epoch以来的秒数）
    time_t epoch_seconds = std::mktime(&tm);
    if (epoch_seconds == -1) {
        throw std::runtime_error("无法将日期转换为time_t类型.");
    }

    // 将秒数转换为天数
    int days_since_epoch = epoch_seconds / (60 * 60 * 24);

    return days_since_epoch;
}