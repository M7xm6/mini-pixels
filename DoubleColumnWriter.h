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

//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_DOUBLECOLUMNWRITER_H
#define DUCKDB_DOUBLECOLUMNWRITER_H
#include <memory>
#include <vector>
#include "TypeDescription.h"
#include "ColumnWriter.h"
#include "PixelsWriterOption.h"
#include "ColumnVector.h"
            // Class to handle writing double type data to a column in the database file.
            class DoubleColumnWriter : public ColumnWriter {
            public:
                // Constructor to initialize the DoubleColumnWriter.
                DoubleColumnWriter(std::shared_ptr<TypeDescription> type,
                    std::shared_ptr<PixelsWriterOption> writerOption);

                // Virtual destructor to ensure proper cleanup.
                virtual ~DoubleColumnWriter() = default;

                // Method to write the data from the given DoubleColumnVector to the file.
                void write(std::shared_ptr<DoubleColumnVector> columnVector) override;

            private:
                void writeBatch(const std::vector<double>& values, const std::vector<bool>& isNulls);
                // Options for writing the pixels file.
                std::shared_ptr<PixelsWriterOption> writerOption_;
            };

   


#endif // DUCKDB_DOUBLECOLUMNWRITER_H
