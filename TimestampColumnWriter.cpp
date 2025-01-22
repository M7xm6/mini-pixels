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

TimestampColumnWriter::TimestampColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption), runlengthEncoding(false), encoder(nullptr) {
    // Check if the type is indeed a TimestampType
    if (!dynamic_cast<TimestampType*>(type.get())) {
        throw std::runtime_error("TimestampColumnWriter can only be used with TimestampType");
    }

    // Decide whether to use run-length encoding based on writer options
    runlengthEncoding = writerOption->useRunLengthEncoding();
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
}

int TimestampColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length) {
    if (!vector || length <= 0) {
        throw std::invalid_argument("Invalid vector or length");
    }

    auto timestampVector = std::dynamic_pointer_cast<TimestampVector>(vector);
    if (!timestampVector) {
        throw std::runtime_error("Expected TimestampVector, but got something else");
    }

    const auto& data = timestampVector->GetData();
    int curPixelLength = 0;
    curPixelVector.clear();

    for (int i = 0; i < length; ++i) {
        curPixelVector.push_back(data[i]);
        curPixelLength++;

        // If we've reached the end of the pixel or the end of the data, write it out
        if (curPixelLength == writerOption->getPixelSize() || i == length - 1) {
            writeCurPartTimestamp(timestampVector, curPixelVector.data(), curPixelLength, i - curPixelLength + 1);
            curPixelLength = 0;
        }
    }

    return length;
}

void TimestampColumnWriter::close() {
    // If there are any remaining values in curPixelVector, write them out
    if (!curPixelVector.empty()) {
        writeCurPartTimestamp(nullptr, curPixelVector.data(), curPixelVector.size(), 0);
    }

    // Close the encoder if it exists
    if (encoder) {
        encoder->Close();
    }

    // Call the base class close method
    ColumnWriter::close();
}

void TimestampColumnWriter::newPixel() {
    // Reset the current pixel vector
    curPixelVector.clear();
}

void TimestampColumnWriter::writeCurPartTimestamp(std::shared_ptr<ColumnVector> columnVector, long* values, int curPartLength, int curPartOffset) {
    if (runlengthEncoding) {
        // Encode using run-length encoding
        encoder->Encode(values, curPartLength);
        // Write the encoded data to the output (this part depends on the actual output mechanism, which is not detailed in the header)
        // For demonstration, we'll just print it out
        for (const auto& encodedValue : encoder->GetEncodedData()) {
            std::cout << "Encoded Value: " << encodedValue << std::endl;
        }
    }
    else {
        // Write the raw values (this part also depends on the actual output mechanism)
        // For demonstration, we'll just print them out
        for (int i = 0; i < curPartLength; ++i) {
            std::cout << "Raw Value: " << values[i] << std::endl;
        }
    }

    // Here you would typically write the data to the actual output stream or file
    // This is a placeholder for the actual writing logic
}

bool TimestampColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
    // Based on the writer options, decide if we need to pad nulls
    // For simplicity, let's assume we always pad nulls here
    return writerOption->padNulls();
}

pixels::proto::ColumnEncoding TimestampColumnWriter::getColumnChunkEncoding() const {
    pixels::proto::ColumnEncoding encoding;
    encoding.set_encodingtype(pixels::proto::ColumnEncoding_Type_RUNLENGTH if runlengthEncoding else pixels::proto::ColumnEncoding_Type_RAW);
    // Add more encoding details if needed
    return encoding;
}