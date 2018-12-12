using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace _2___DataStats
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var con = new SqlConnection("Server=.;Database=WideWorldImporters;Trusted_Connection=True;"))
            {
                con.Open();
                var procTexts = GetStoredProcedures(con)
                    .Select(n => new { ProcName = n, ProcText = GetProcText(con, n) })
                    .ToList();
                var procTrees = procTexts.Select(p =>
                {
                    var processed = ParseSql(p.ProcText);
                    var visitor = new StatsVisitor();
                    if (!processed.errors.Any())
                        processed.sqlTree.Accept(visitor);
                    return new { p.ProcName, processed.sqlTree, processed.errors, visitor };
                }).ToList();
                var visitors = procTrees.Select(p => p.visitor);
                foreach (var procTree in procTrees)
                {
                    Console.WriteLine(procTree.ProcName);
                    if (procTree.errors.Any())
                    {
                        Console.WriteLine("   Errors found:");
                        foreach (var error in procTree.errors)
                        {
                            Console.WriteLine($"     Line: {error.Line}  Col: {error.Column}: {error.Message}");
                        }
                    }
                    else
                    {
                        var visitor = procTree.visitor;
                        Console.WriteLine($"  Inserts: {visitor.Inserts}");
                        foreach (var table in visitor.InsertTables)
                        {
                            Console.WriteLine($"      {table}");
                        }

                        Console.WriteLine($"  Updates: {visitor.Updates}");
                        foreach (var table in visitor.UpdateTables)
                        {
                            Console.WriteLine($"      {table}");
                        }

                        Console.WriteLine($"  Deletes: {visitor.Deletes}");
                        foreach (var table in visitor.DeleteTables)
                        {
                            Console.WriteLine($"      {table}");
                        }

                        Console.WriteLine($"  Creates: {visitor.Creates}");
                        foreach (var table in visitor.CreateTables)
                        {
                            Console.WriteLine($"      {table}");
                        }

                        Console.WriteLine($"  Drops: {visitor.Drops}");
                        foreach (var table in visitor.DropTables)
                        {
                            Console.WriteLine($"      {table}");
                        }
                    }
                }
            }

            Console.ReadLine();
        }

        private static List<string> GetStoredProcedures(SqlConnection con)
        {
            using (SqlCommand sqlCommand = new SqlCommand(
                "select s.name+'.'+p.name as name from sys.procedures p " + 
                "inner join sys.schemas s on p.schema_id = s.schema_id order by name",
                con))
            {
                using (DataTable procs = new DataTable())
                {
                    procs.Load(sqlCommand.ExecuteReader());
                    return procs.Rows.OfType<DataRow>().Select(r => r.Field<String>("name")).ToList();
                }
            }
        }

        private static string GetProcText(SqlConnection con, string procName)
        {
            using (SqlCommand sqlCommand = new SqlCommand("sys.sp_helpText", con)
            {
                CommandType = CommandType.StoredProcedure
            })
            {
                sqlCommand.Parameters.AddWithValue("@objname", procName);
                using (var proc = new DataTable())
                {
                    try
                    {
                        proc.Load(sqlCommand.ExecuteReader());
                        return string.Join("", proc.Rows.OfType<DataRow>().Select(r => r.Field<string>("Text")));
                    }
                    catch (SqlException)
                    {
                        return null;
                    }
                }
            }
        }

        private static (TSqlFragment sqlTree, IList<ParseError> errors) ParseSql(string procText)
        {
            var parser = new TSql150Parser(true);
            using (var textReader = new StringReader(procText))
            {
                var sqlTree = parser.Parse(textReader, out var errors);

                return (sqlTree, errors);
            }
        }
    }

    internal class StatsVisitor : TSqlFragmentVisitor
    {
        public int Inserts { get; private set; }
        public int Updates { get; private set; }
        public int Deletes { get; private set; }
        public int Creates { get; private set; }
        public int Drops { get; private set; }
        public List<string> InsertTables { get; }
        public List<string> UpdateTables { get; }
        public List<string> DeleteTables { get; }
        public List<string> CreateTables { get; }
        public List<string> DropTables { get; }

        public StatsVisitor()
        {
            InsertTables = new List<string>();
            UpdateTables = new List<string>();
            DeleteTables = new List<string>();
            CreateTables = new List<string>();
            DropTables = new List<string>();
        }

        public override void Visit(InsertStatement node)
        {
            Inserts++;
            var targetName = (node.InsertSpecification.Target as NamedTableReference)?.
                SchemaObject.BaseIdentifier.Value ??
              (node.InsertSpecification.Target as VariableTableReference)?.Variable.Name;
            InsertTables.Add(targetName);
        }

        public override void Visit(UpdateStatement node)
        {
            Updates++;
            UpdateTables.Add((node.UpdateSpecification.Target as NamedTableReference)?.
                SchemaObject.BaseIdentifier.Value);
        }

        public override void Visit(DeleteStatement node)
        {
            Deletes++;
            UpdateTables.Add((node.DeleteSpecification.Target as NamedTableReference)?.
                SchemaObject.BaseIdentifier.Value);
        }

        public override void Visit(CreateTableStatement node)
        {
            Creates++;
            CreateTables.Add(node.SchemaObjectName.BaseIdentifier.Value);
        }

        public override void Visit(DropTableStatement node)
        {
            Drops++;
            DropTables.AddRange(node.Objects.Select(o => o.BaseIdentifier.Value));
        }

    }
}
