using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections;
using System.IO;
using System.Linq;
using System.Text;

namespace sqlschema {
    class Program {
		const string DefaultOutputFolder = "";
		const string DefaultServer = "localhost";

		//TODO: swap out for System.CommandLine once ready
		static int Main(string[] args) {
			static int PrintUsage() {
				Console.ForegroundColor = ConsoleColor.DarkRed;
				Console.Error.WriteLine("sqlschema.exe -d db_name [-o output_folder]");
				Console.ResetColor();
				return 1;
			}
			string dbName, outputFolder = "";
			if (args.Length >= 2) {
				if (args[0] != "-d") {
					return PrintUsage();
				}
				dbName = args[1];
				if (String.IsNullOrWhiteSpace(dbName)) {
					return PrintUsage();
				}
				if (args.Length > 2) {
					if (args.Length != 4 || args[2] != "-o") {
						return PrintUsage();
					}
					outputFolder = args[3];
					if (String.IsNullOrWhiteSpace(outputFolder)) {
						outputFolder = DefaultOutputFolder;
					}
				}
				ScriptDatabase(dbName, outputFolder, DefaultServer);
				return 0; //TODO: only if no exception...
			}
			return PrintUsage();
		}

		private static void ScriptDatabase(string dbName, string outputFolder, string serverName) {
			var connectionInfo = new SqlConnectionInfo {
				ServerName = serverName,
				UseIntegratedSecurity = true
			};
			var connection = new ServerConnection(connectionInfo);
			var server = new Server(connection);
			var database = server.Databases[dbName];
			if (database == null) {
				throw new ArgumentException("Database not found", nameof(dbName));
			}
			ScriptDatabase(database, outputFolder);
		}

		private static void ScriptDatabase(Database db, string outputFolder) {
			var dir = outputFolder;
			ScriptObjects<Table>(db.Tables, dir, "Tables");
			ScriptObjects<View>(db.Views, dir, "Views");
			ScriptObjects<Sequence>(db.Sequences, dir, "Sequences");
			ScriptObjects<StoredProcedure>(db.StoredProcedures, dir, "Stored Procedures");

			var fdir = Path.Combine(dir, "Functions");
			var fobjs = db.UserDefinedFunctions.Cast<UserDefinedFunction>().ToArray();
			ScriptObjects<UserDefinedFunction>(fobjs.Where(f => f.FunctionType != UserDefinedFunctionType.Scalar), fdir, "Table-valued Functions");
			ScriptObjects<UserDefinedFunction>(fobjs.Where(f => f.FunctionType == UserDefinedFunctionType.Scalar), fdir, "Scalar-valued Functions");
			ScriptObjects<UserDefinedAggregate>(db.UserDefinedAggregates, fdir, "Aggregate Functions");

			var tdir = Path.Combine(dir, "Types");
			ScriptObjects<UserDefinedDataType>(db.UserDefinedDataTypes, tdir, "User-Defined Data Types");
			ScriptObjects<UserDefinedTableType>(db.UserDefinedTableTypes, tdir, "User-Defined Table Types");
			ScriptObjects<UserDefinedType>(db.UserDefinedTypes, tdir, "User-Defined Types");

			ScriptObjects<DatabaseRole>(db.Roles.Cast<DatabaseRole>().Where(r => !IsSystemRole(r.Name)), dir, "Roles");
			ScriptObjects<Schema>(db.Schemas, dir, "Schemas");
		}

		private static void ScriptObjects<T>(IEnumerable objects, string parentFolder, string folderName) where T : ScriptNameObjectBase, IScriptable {
			var objs = from o in objects.Cast<T>()
					   let ps = o.Properties
					   let p = ps.Contains("IsSystemObject") ? ps["IsSystemObject"] : null
					   where !Convert.ToBoolean(p?.Value ?? false)
					   select o;
			var outputFolder = Path.Combine(parentFolder, folderName);
			ScriptObjects(objs.ToArray(), outputFolder);
		}

		private static void ScriptObjects<T>(T[] objects, string outputFolder) where T : ScriptNameObjectBase, IScriptable {
			if (!objects.Any()) {
				return;
			}

			Directory.CreateDirectory(outputFolder);

			foreach (var o in objects) {
				ScriptObject(o, outputFolder);
			}
		}

		private static void ScriptObject<T>(T o, string outputFolder) where T : ScriptNameObjectBase, IScriptable {
			var filename = o is ScriptSchemaObjectBase so ? $"{so.Schema}.{so.Name}.sql" : $"{o.Name}.sql";
			var path = Path.Combine(outputFolder, filename);
			using var file = File.CreateText(path);
			var create = o.Script(ScriptCreate);
			foreach (var s in create) {
				const string dontWant = "WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)";
				var s2 = s.Contains(dontWant) ? s.Replace(dontWant, "") : s;
				file.WriteLine(s2);
			}
		}

		static readonly ScriptingOptions ScriptCreate = new() {
			AllowSystemObjects = false,
			ChangeTracking = true,
			ClusteredIndexes = true,
			ColumnStoreIndexes = true,
			Default = true,
			DriAll = true,
			Encoding = Encoding.UTF8,
			FullTextCatalogs = true,
			FullTextIndexes = true,
			IncludeDatabaseContext = false,
			IncludeHeaders = false,
			IncludeScriptingParametersHeader = false,
			Indexes = true,
			NonClusteredIndexes = true,
			NoCollation = true,
			NoFileGroup = true,
			NoTablePartitioningSchemes = true,
			NoXmlNamespaces = true,
			Permissions = true,
			SchemaQualify = true,
			ScriptData = false,
			ScriptDrops = false,
			ScriptSchema = true,
			SpatialIndexes = true,
			Statistics = false,
			Triggers = true,
			WithDependencies = false,
			XmlIndexes = true,
		};

		//roles don't have IsSystemObject so need to do it ourselves
		static readonly string[] SystemRoles = "db_accessadmin,db_backupoperator,db_datareader,db_datawriter,db_ddladmin,db_denydatareader,db_denydatawriter,db_executor,db_owner,db_securityadmin,public".Split(',');
		static bool IsSystemRole(string role) => Array.BinarySearch(SystemRoles, role) >= 0;

	}
}
