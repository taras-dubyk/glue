const { CLI, CREATE_TABLE } = require('./cliConstants');
const { getGlueTableColumns, getGluePartitionKeyTableColumns, getGlueTableClusteringKeyColumns, getGlueTableSortingColumns } = require('./glueColumnHelper');

const getGlueTableCreateStatement = (tableSchema, databaseName) => {
	const tableParameters = {
		DatabaseName: databaseName,
		TableInput: {
			Name: tableSchema.title,
			Description: tableSchema.description,
			// Owner: '',
			// Retention: 0,
			StorageDescriptor: {
				Columns: getGlueTableColumns(tableSchema.properties),
				Location: tableSchema.location,
				InputFormat: '',
				OutputFormat: '',
				Compressed: tableSchema.compressed,
				NumberOfBuckets: tableSchema.numBuckets,
				// SerdeInfo: {
				// 	Name: '',
				// 	SerializationLibrary: '',
				// 	Parameters: {
				// 		KeyName: ''
				// 	}
				// },
				BucketColumns: getGlueTableClusteringKeyColumns(tableSchema.properties),
				SortColumns: getGlueTableSortingColumns(tableSchema.sortedByKey, tableSchema.properties),
				// Parameters: {
				// 	KeyName: ''
				// },
				// SkewedInfo: {
				// 	SkewedColumnNames: [''],
				// 	SkewedColumnValues: [''],
				// 	SkewedColumnValueLocationMaps: {
				// 		KeyName: ''
				// 	}
				// },
				// StoredAsSubDirectories: true
			},
			PartitionKeys: getGluePartitionKeyTableColumns(tableSchema.properties),
			// TableType: '',
			// Parameters: {
			// 	KeyName: ''
			// }
		}
	};

	const cliStatement = `${CLI} ${CREATE_TABLE} '${JSON.stringify(tableParameters, null, 2)}'`;
	return cliStatement;
};

module.exports = {
	getGlueTableCreateStatement
};
