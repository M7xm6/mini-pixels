/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#include "DoubleColumnWriter.h"
#include <fstream>
#include <iostream>
#include <stdexcept>

 // 构造函数，初始化DoubleColumnWriter
DoubleColumnWriter::DoubleColumnWriter(std::shared_ptr<TypeDescription> type,
    std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type), writerOption_(writerOption) {
    // 检查类型是否为DOUBLE类型
    if (type->getType() != TypeId::DOUBLE) {
        throw std::invalid_argument("Type mismatch for DoubleColumnWriter");
    }
}

void DoubleColumnWriter::write(std::shared_ptr<DoubleColumnVector> columnVector) {
    const auto& values = columnVector->getValues();
    const auto& isNull = columnVector->getIsNull();

    writeBatch(values, isNull);
}


void DoubleColumnWriter::writeBatch(const std::vector<double>& values, const std::vector<bool>& isNull) {
    // 写入数据
    for (size_t i = 0; i < values.size(); ++i) {
        if (isNull[i]) {
            // 处理空值
            std::cerr << "Null value encountered at index " << i << ", handling null values is not implemented." << std::endl;
   
        }
        else {            
            std::cout <<values[i] << std::endl;
        }
    }

    