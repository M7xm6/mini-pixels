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
#include "decimalColumnWriter.h"
#include <vector>
#include <iostream>
#include <stdexcept>

DecimalColumnWriter::DecimalColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption) {
    // Initialization if needed
}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length) {
    // Ensure the vector is of the correct type
    auto decimalVector = std::dynamic_pointer_cast<DecimalColumnVector>(vector);
    if (!decimalVector) {
        throw std::invalid_argument("Expected DecimalColumnVector, but got something else.");
    }

    // Extract the values and null flags from the vector
    const std::vector<int64_t>& values = decimalVector->getData();
    const std::vector<bool>& isNull = decimalVector->getNullFlags();

    // Byte order
    bool littleEndian = (writerOption->getByteOrder() == ByteOrder::LITTLE_ENDIAN);

    // Iterate over the values and write them to the output stream
    for (int i = 0; i < length; ++i) {
        // Set the null flag
        curPixelIsNull[curPixelIsNullIndex++] = isNull[i];
        curPixelEleIndex++;

        if (isNull[i]) {
            hasNull = true;
            pixelStatRecorder->increment();
            if (nullsPadding) {
                if (littleEndian) {
                    EncodingUtils::writeLongLE(outputStream, 0L);
                }
                else {
                    EncodingUtils::writeLongBE(outputStream, 0L);
                }
            }
        }
        else {
            int64_t value = values[i];

            if (littleEndian) {
                EncodingUtils::writeLongLE(outputStream, value);
            }
            else {
                EncodingUtils::writeLongBE(outputStream, value);
            }

            pixelStatRecorder->updateInteger(value, 1);
        }

        
        if (curPixelEleIndex >= pixelStride) {
            newPixel();
        }
    }

    return outputStream->size();
}

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    return writerOption->isNullsPadding();
}