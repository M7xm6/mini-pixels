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
#include "DateColumnWriter.h"
#include "ColumnVector.h"
#include "DateColumnVector.h"
#include "PixelsWriterOption.h"
#include "TypeDescription.h"
#include "utils/EncodingUtils.h"
#include <iostream>
#include <vector>
#include <memory>

DateColumnWriter::DateColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption), runlengthEncoding(writerOption->getEncodingLevel().ge(EncodingLevel::EL2)) {
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>(true, true);
    }
}

int DateColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length) {
    auto dateColumnVector = std::dynamic_pointer_cast<DateColumnVector>(vector);
    if (!dateColumnVector) {
        throw std::runtime_error("Invalid column vector type for DateColumnWriter");
    }

    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = length;

    // Process the data in chunks based on pixel stride
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartTime(dateColumnVector, dateColumnVector->values.data(), curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = length - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartTime(dateColumnVector, dateColumnVector->values.data(), curPartLength, curPartOffset);

    return outputStream->size();
}

void DateColumnWriter::writeCurPartTime(std::shared_ptr<ColumnVector> columnVector, int* values, int curPartLength, int curPartOffset) {
    auto dateColumnVector = std::dynamic_pointer_cast<DateColumnVector>(columnVector);
    if (!dateColumnVector) {
        throw std::runtime_error("Invalid column vector type for DateColumnWriter");
    }

    for (int i = 0; i < curPartLength; ++i) {
        curPixelEleIndex++;
        if (dateColumnVector->isNull[i + curPartOffset]) {
            hasNull = true;
            pixelStatRecorder->increment();
            if (nullsPadding) {
                curPixelVector.push_back(0);
            }
        }
        else {
            curPixelVector.push_back(values[i + curPartOffset]);
        }
    }

    // Copy the null flags for the current part
    for (int i = 0; i < curPartLength; ++i) {
        isNull[curPixelIsNullIndex++] = dateColumnVector->isNull[i + curPartOffset];
    }
}

void DateColumnWriter::newPixel() {
    if (runlengthEncoding) {
        for (int i = 0; i < curPixelVector.size(); ++i) {
            pixelStatRecorder->updateDate(curPixelVector[i]);
        }
        std::vector<uint8_t> encodedData = encoder->encode(curPixelVector.data(), curPixelVector.size());
        outputStream->write(encodedData.data(), encodedData.size());
    }
    else {
        for (int i = 0; i < curPixelVector.size(); ++i) {
            int32_t value = curPixelVector[i];
            outputStream->writeInt32(value);
            pixelStatRecorder->updateDate(value);
        }
    }

    ColumnWriter::newPixel();
    curPixelVector.clear();
}

pixels::proto::ColumnEncoding DateColumnWriter::getColumnChunkEncoding() const {
    pixels::proto::ColumnEncoding encoding;
    if (runlengthEncoding) {
        encoding.set_kind(pixels::proto::ColumnEncoding::RUNLENGTH);
    }
    else {
        encoding.set_kind(pixels::proto::ColumnEncoding::NONE);
    }
    return encoding;
}

void DateColumnWriter::close() {
    if (runlengthEncoding) {
        encoder->close();
    }
    ColumnWriter::close();
}

bool DateColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    if (writerOption->getEncodingLevel().ge(EncodingLevel::EL2)) {
        return false;
    }
    return writerOption->isNullsPadding();
}