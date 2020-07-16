using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableDataStore
{
    /// <summary>
    /// Converts the <c>entity => new { entity.Prop1, entity.Prop2 ... }</c> expressions into a list of
    /// string that contain the property names.
    /// </summary>
    public class AzureStorageQuerySelectTranslator : ExpressionVisitor
    {

        private EntityPropertyConverterOptions _options = new EntityPropertyConverterOptions();
        private List<string> _memberNames = new List<string>();
        private int _memberDepth = 0;
        private string _currentMemberName = "";

        public static List<string> TranslateExpressionToMemberNames(Expression e,
            EntityPropertyConverterOptions options)
        {
            var translator = new AzureStorageQuerySelectTranslator();
            translator._options = options;

            translator.Visit(e);
            return translator._memberNames;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            foreach (var argument in node.Arguments)
            {
                Visit(argument);
            }

            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null && node.Expression.NodeType == ExpressionType.Parameter)
            {
                _currentMemberName += node.Member.Name;
                if (_memberDepth == 0)
                {
                    _memberNames.Add(_currentMemberName);
                    _currentMemberName = "";
                }

                return node;
            }
            else if (node.Expression != null && node.Expression.NodeType == ExpressionType.MemberAccess)
            {
                MemberExpression innerExpression = node.Expression as MemberExpression;
                _memberDepth++;
                VisitMember(innerExpression);
                _currentMemberName += _options.PropertyNameDelimiter + node.Member.Name;
                _memberDepth--;
                if (_memberDepth == 0)
                {
                    _memberNames.Add(_currentMemberName);
                    _currentMemberName = "";
                }

                return node;
            }

            throw new NotSupportedException("Expression not supported");
            //return base.VisitMember(node);
        }
    }
}