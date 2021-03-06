﻿// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace CassandraSharp.CQLOrdinal
{
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;
    using System.Collections.Generic;
    using System.Linq;

    internal sealed class OrdinalDataMapper : IDataMapper
    {
        /// <summary>
        /// bcz we do not have properties, we could use a static object to share every threads.
        /// </summary>
        public static OrdinalDataMapper DefaultInstance = new OrdinalDataMapper();

        public IEnumerable<IColumnData> MapToColumns(object dataSource, IEnumerable<IColumnSpec> columns)
        {
            var data = (object[])dataSource;

            foreach (var column in columns)
            {
                var value = data[column.Index];

                byte[] rawData = null;
                if (value != null)
                {
                    rawData = column.Serialize(value);
                }

                yield return new ColumnData(column, rawData);
            }
        }

        public object MapToObject(IEnumerable<IColumnData> rowData)
        {
            var rows = rowData.ToList();

            var instance = new object[rows.Count];
            foreach (var column in rows.Where(r => r.RawData != null))
            {
                instance[column.ColumnSpec.Index] = column.ColumnSpec.Deserialize(column.RawData);
            }

            return instance;
        }
    }
}