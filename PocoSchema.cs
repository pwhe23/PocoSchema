
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Text;
using PetaPoco;

namespace PocoSchema
{
	public class DbSchema
	{
		private readonly Database _db;

		public DbSchema(Database db)
		{
			_db = db;
			DefaultSchema = "dbo";
			DefaultStringType = "varchar";
			DefaultStringLength = 50;
			Tables = new List<DbTable>();
		}

		public string ConnectionString { get; set; }
		public string DefaultSchema { get; set; }
		public string DefaultStringType { get; set; }
		public int DefaultStringLength { get; set; }
		public List<DbTable> Tables { get; set; }

		public void AddTable(Type type)
		{
			if (type == null)
				return;

			var table = new DbTable();

			//Name, Schema
			var ta = GetAttribute<TableAttribute>(type);
			if (ta != null)
			{
				table.Schema = ta.Schema;
				table.Name = ta.Name;
			}
			if (string.IsNullOrWhiteSpace(table.Name))
			{
				table.Name = type.Name;
			}
			if (string.IsNullOrWhiteSpace(table.Schema))
			{
				table.Schema = DefaultSchema;
			}

			//Columns
			foreach (var prop in type.GetProperties())
			{
				AddColumn(table, prop);
				AddIndex(table, prop);
			}

			//Key
			if (!table.Columns.Any(x => x.IsKey))
				throw new ApplicationException("Table must have a key: " + table.Name);

			Tables.Add(table);
		}
		
		private void AddColumn(DbTable table, PropertyInfo prop)
		{
			//remove existing column
			var existing = table.Columns.FirstOrDefault(x => x.Name == prop.Name);
			if (existing != null)
			{
				table.Columns.Remove(existing);
			}

			var column = new DbColumn();
			
			//Name
			column.Name = prop.Name;
			column.Type = prop.PropertyType;
			column.DbType = GetDbType(column.Type.ToString());

			//Nullable
			if (prop.PropertyType != typeof(string) && !prop.PropertyType.Name.Contains("Nullable`1"))
			{
				column.IsNullable = false;
			}

			var ra = GetAttribute<RequiredAttribute>(prop);
			if (ra != null)
			{
				column.IsNullable = false;
			}

			//Length
			var sl = GetAttribute<StringLengthAttribute>(prop);
			if (sl != null)
			{
				column.Length = sl.MaximumLength;
			}
			if (!column.Length.HasValue && column.Type == typeof (string))
			{
				column.Length = DefaultStringLength;
			}

			//Key
			var ka = GetAttribute<KeyAttribute>(prop);
			if (ka != null)
			{
				column.IsKey = true;
				column.IsNullable = false;
			}

			var ga = GetAttribute<DatabaseGeneratedAttribute>(prop);
			if (ga != null)
			{
				if (ga.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity)
					column.IsIdentity = true;
			}

			table.Columns.Add(column);
		}
		
		private void AddIndex(DbTable table, PropertyInfo prop)
		{
			var indexAttr = GetAttribute<IndexAttribute>(prop);
			if (indexAttr == null)
				return;

			var index = table.Indexes.SingleOrDefault(x => x.Name == indexAttr.Name);
			if (index == null)
			{
				index = new DbIndex
					{
						Name = indexAttr.Name,
						Schema = table.Schema,
						Table = table.Name,
						IsUnique = indexAttr.IsUnique,
					};
				table.Indexes.Add(index);
			}

			index.Columns.Add(prop.Name);
		}

		public string GenerateSql()
		{
			var sb = new StringBuilder();
			sb.AppendFormat("-- Migration Generated: {0:M/d/yyyy h:mm:ss tt} --\r\n", DateTime.Now);

			var dbtables = GetDatabaseTables();
			var dbindexes = GetDatabaseIndexes();

			foreach (var table in Tables)
			{
				var tableExists = dbtables.Any(x => x.Schema == table.Schema && x.Name == table.Name);
				
				sb.AppendFormat("-- Table: {0} --\r\n", table.Name);
				AppendTableSql(sb, table, tableExists);
				AppendIndexSql(sb, table, dbindexes);
			}

			return sb.ToString();
		}

		public void Execute()
		{
			_db.Execute(GenerateSql());
		}

		private List<DbTable> GetDatabaseTables()
		{
			return _db.Query<DbTable>(@"
				SELECT TABLE_SCHEMA AS [Schema], TABLE_NAME AS [Name]
				FROM information_schema.tables
				WHERE TABLE_TYPE = 'BASE TABLE'
			").ToList();
		}

		private List<DbIndex> GetDatabaseIndexes()
		{
			//REF: http://stackoverflow.com/questions/765867/list-of-all-index-index-columns-in-sql-server-db
			var indexes = _db.Query<dynamic>(@"
				SELECT
				  schema_name(schema_id) as SchemaName, OBJECT_NAME(si.object_id) as TableName, si.name as IndexName,
				  (CASE is_primary_key WHEN 1 THEN 'PK' ELSE '' END) as PK,
				  (CASE is_unique WHEN 1 THEN '1' ELSE '0' END)+' '+
				  (CASE si.type WHEN 1 THEN 'C' WHEN 3 THEN 'X' ELSE 'B' END)+' '+  -- B=basic, C=Clustered, X=XML
				  (CASE INDEXKEY_PROPERTY(si.object_id,index_id,1,'IsDescending') WHEN 0 THEN 'A' WHEN 1 THEN 'D' ELSE '' END)+
				  (CASE INDEXKEY_PROPERTY(si.object_id,index_id,2,'IsDescending') WHEN 0 THEN 'A' WHEN 1 THEN 'D' ELSE '' END)+
				  (CASE INDEXKEY_PROPERTY(si.object_id,index_id,3,'IsDescending') WHEN 0 THEN 'A' WHEN 1 THEN 'D' ELSE '' END)+
				  (CASE INDEXKEY_PROPERTY(si.object_id,index_id,4,'IsDescending') WHEN 0 THEN 'A' WHEN 1 THEN 'D' ELSE '' END)+
				  (CASE INDEXKEY_PROPERTY(si.object_id,index_id,5,'IsDescending') WHEN 0 THEN 'A' WHEN 1 THEN 'D' ELSE '' END)+
				  (CASE INDEXKEY_PROPERTY(si.object_id,index_id,6,'IsDescending') WHEN 0 THEN 'A' WHEN 1 THEN 'D' ELSE '' END)+
				  '' as 'Type',
				  INDEX_COL(schema_name(schema_id)+'.'+OBJECT_NAME(si.object_id),index_id,1) as Key1,
				  INDEX_COL(schema_name(schema_id)+'.'+OBJECT_NAME(si.object_id),index_id,2) as Key2,
				  INDEX_COL(schema_name(schema_id)+'.'+OBJECT_NAME(si.object_id),index_id,3) as Key3,
				  INDEX_COL(schema_name(schema_id)+'.'+OBJECT_NAME(si.object_id),index_id,4) as Key4,
				  INDEX_COL(schema_name(schema_id)+'.'+OBJECT_NAME(si.object_id),index_id,5) as Key5,
				  INDEX_COL(schema_name(schema_id)+'.'+OBJECT_NAME(si.object_id),index_id,6) as Key6
				FROM sys.indexes as si
				LEFT JOIN sys.objects as so on so.object_id=si.object_id
				WHERE index_id>0 -- omit the default heap
				  and OBJECTPROPERTY(si.object_id,'IsMsShipped')=0 -- omit system tables
				  and not (schema_name(schema_id)='dbo' and OBJECT_NAME(si.object_id)='sysdiagrams') -- omit sysdiagrams
				ORDER BY SchemaName,TableName,IndexName
			");

			var list = new List<DbIndex>();
			foreach (var x in indexes)
			{
				var index = new DbIndex
					{
						Name = x.IndexName,
						Schema = x.SchemaName,
						Table = x.TableName,
					};

				if (x.Key1 != null) index.Columns.Add(x.Key1);
				if (x.Key2 != null) index.Columns.Add(x.Key2);
				if (x.Key3 != null) index.Columns.Add(x.Key3);
				if (x.Key4 != null) index.Columns.Add(x.Key4);
				if (x.Key5 != null) index.Columns.Add(x.Key5);
				if (x.Key6 != null) index.Columns.Add(x.Key6);
				
				list.Add(index);
			}
			return list;
		}

		private List<DbColumn> GetDatabaseColumns(DbTable table)
		{
			var columns = _db.Query<DbColumn>(@"
				SELECT c.COLUMN_NAME As [Name], c.CHARACTER_MAXIMUM_LENGTH AS [Length], c.DATA_TYPE AS [DbType],
					(CASE WHEN c.IS_NULLABLE='YES' THEN 1 ELSE 0 END) AS [IsNullable],
					(CASE WHEN pk.CONSTRAINT_TYPE='Primary Key' THEN 1 ELSE 0 END) AS IsKey,
					COLUMNPROPERTY(object_id(c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IsIdentity
				FROM INFORMATION_SCHEMA.COLUMNS c
				LEFT JOIN (
					SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, ccu.COLUMN_NAME, tc.CONSTRAINT_TYPE
					FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
					LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.Constraint_name
					WHERE tc.CONSTRAINT_TYPE = 'Primary Key'
				) pk ON (pk.TABLE_SCHEMA=c.TABLE_SCHEMA AND pk.TABLE_NAME=c.TABLE_NAME AND pk.COLUMN_NAME=c.COLUMN_NAME)
				WHERE c.TABLE_SCHEMA=@0 AND c.TABLE_NAME=@1
			", table.Schema, table.Name).ToList();
			
			columns.ForEach(x =>
				{
					if (x.Length == -1) x.Length = int.MaxValue;
				});
			
			return columns;
		}

		private void AppendTableSql(StringBuilder sb, DbTable table, bool exists)
		{
			if (!exists)
			{
				sb.AppendFormat("CREATE TABLE [{0}].[{1}] (\r\n", table.Schema, table.Name);

				for (var i = 0; i < table.Columns.Count; i++)
				{
					var column = table.Columns[i];
					var lastcol = i == table.Columns.Count - 1;
					sb.AppendFormat("\t{0}{1}\r\n", GetColumnSql(column, false), lastcol ? "" : ",");
				}

				sb.AppendFormat(");\r\n");
			}
			else
			{
				var dbcolumns = GetDatabaseColumns(table);
				foreach (var col in table.Columns)
				{
					var existing = dbcolumns.SingleOrDefault(x => x.Name == col.Name);
					if (existing == null)
					{
						//add column
						sb.AppendFormat("ALTER TABLE [{0}].[{1}] ADD {2};\r\n",
										table.Schema, table.Name, GetColumnSql(col, true));
					}
					else if (existing.DbType == col.DbType && existing.Length == col.Length
							 && existing.IsIdentity == col.IsIdentity && existing.IsKey == col.IsKey
							 && existing.IsNullable == col.IsNullable)
					{
						//ignore
						continue;
					}
					else
					{
						//modify column
						sb.AppendFormat("ALTER TABLE {0}.[{1}] ALTER COLUMN {2};\r\n",
										table.Schema, table.Name, GetColumnSql(col, true));
					}
				}
			}
		}

		private string GetColumnSql(DbColumn column, bool alter)
		{
			var def = GetColumnDefault(column, alter);

			return string.Format("[{0}] {1} {2} {3} {4} {5}",
								 column.Name,
								 GetColumnType(column),
								 column.IsIdentity ? "IDENTITY" : "",
								 column.IsNullable ? "NULL" : "NOT NULL",
								 column.IsKey ? "PRIMARY KEY" : "",
								 def
				);
		}

		private void AppendIndexSql(StringBuilder sb, DbTable table, List<DbIndex> dbindexes)
		{
			foreach (var index in table.Indexes)
			{
				if (dbindexes.Any(x => x.Schema == index.Schema && x.Table == index.Table && x.Name == index.Name))
					continue;

				sb.AppendFormat("CREATE {0} INDEX [{1}] ON [{2}].[{3}] ([{4}]);\r\n",
								index.IsUnique ? "UNIQUE" : "",
				                index.Name, 
								index.Schema, 
								index.Table, 
								string.Join("],[", index.Columns)
				);
			}
		}
		
		private string GetColumnType(DbColumn column)
		{
			var dbtype = GetDbType(column.Type.ToString());
			switch (column.Type.ToString())
			{
				case "System.String":
					if (column.Length == int.MaxValue)
						return dbtype + "(MAX)";
					else
						return dbtype + "(" + column.Length + ")";
				default:
					return dbtype;
			}
		}

		private string GetColumnDefault(DbColumn column, bool alter)
		{
			if (column.Default != null)
			{
				return "DEFAULT " + column.Default;
			}
			else if (!alter || column.IsNullable)
			{
				return null; //ignore
			}
			else if (column.Type == typeof (int))
			{
				return "DEFAULT " + default(int);
			}
			else if (column.Type == typeof (bool))
			{
				return "DEFAULT 0";
			}
			return null;
		}

		private string GetDbType(string type)
		{
			switch (type)
			{
				case "System.String":
					return DefaultStringType;
				case "System.Int32":
				case "System.Nullable`1[System.Int32]":
					return "int";
				case "System.Boolean":
				case "System.Nullable`1[System.Boolean]":
					return "bit";
				case "System.DateTime":
				case "System.Nullable`1[System.DateTime]":
					return "datetime";
				default:
					throw new ApplicationException("Column type unknown: " + type);
			}
		}

		private static T GetAttribute<T>(Type type, bool inherit = false) where T:Attribute
		{
			return (T)type.GetCustomAttributes(typeof(T), inherit).FirstOrDefault();
		}

		private static T GetAttribute<T>(MemberInfo member, bool inherit = false) where T:Attribute
		{
			return (T) member.GetCustomAttributes(typeof (T), inherit).FirstOrDefault();
		}
	};

	public class DbTable
	{
		public DbTable()
		{
			Columns = new List<DbColumn>();
			Indexes = new List<DbIndex>();
		}
		public string Schema { get; set; }
		public string Name { get; set; }
		public List<DbColumn> Columns { get; set; }
		public List<DbIndex> Indexes { get; set; }
	};

	public class DbColumn
	{
		public DbColumn()
		{
			IsNullable = true;
		}
		public string Name { get; set; }
		public int? Length { get; set; }
		public bool IsKey { get; set; }
		public bool IsIdentity { get; set; }
		public bool IsNullable { get; set; }
		public Type Type { get; set; }
		public string DbType { get; set; }
		public string Default { get; set; }
	};

	public class DbIndex
	{
		public DbIndex()
		{
			Columns = new List<string>();
		}
		public string Name { get; set; }
		public string Schema { get; set; }
		public string Table { get; set; }
		public bool IsUnique { get; set; }
		public List<string> Columns { get; set; }
	}
}

namespace System.ComponentModel.DataAnnotations.Schema
{
	//REF: http://blogs.southworks.net/dschenkelman/2012/08/18/creating-indexes-via-data-annotations-with-entity-framework-5-0/
	[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = true)]
	public class IndexAttribute : Attribute
	{
		public IndexAttribute(string name, bool unique = false)
		{
			Name = name;
			IsUnique = unique;
		}

		public string Name { get; set; }

		public bool IsUnique { get; set; }
	};
}
