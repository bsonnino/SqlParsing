using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace _1___SimpleParsing
{
    class Program
    {
        static void Main(string[] args)
        {
            var sql = "Select * from customer";
            var parsed = ParseSql(sql);
            if (parsed.errors.Any())
                return;
            var visitor = new SelectVisitor();
            parsed.sqlTree.Accept(visitor);
            Console.ReadLine();
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

    internal class SelectVisitor : TSqlFragmentVisitor
    {
        public override void ExplicitVisit(SelectStatement node)
        {
            Console.WriteLine($"Visiting Select: {node}");
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(QueryExpression node)
        {
            Console.WriteLine($"Visiting QueryExpression: {node}");
        }

        public override void ExplicitVisit(QuerySpecification node)
        {
            Console.WriteLine($"Visiting QuerySpecification: {node}");
        }

        public override void ExplicitVisit(SelectStarExpression node)
        {
            Console.WriteLine($"Visiting SelectStarExpression: {node}");
        }
    }
}
