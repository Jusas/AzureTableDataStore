using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableDataStore
{
    internal class AzureStorageQueryTranslator : ExpressionVisitor
    {
        private StringBuilder Filter = new StringBuilder();
        private EntityPropertyConverterOptions _options = new EntityPropertyConverterOptions();

        private string _partitionKeyProperty;
        private string _rowKeyProperty;

        private int _memberDepth = 0;
        private MemberExpression _lastVisitedMemberExpression = null;

        private enum BinarySide
        {
            None,
            Left,
            Right
        }

        private Stack<BinarySide> _binarySideStack = new Stack<BinarySide>(new []{BinarySide.None});

        private AzureStorageQueryTranslator(string partitionKeyProperty, string rowKeyProperty)
        {
            _partitionKeyProperty = partitionKeyProperty;
            _rowKeyProperty = rowKeyProperty;
        }

        private bool IsNullConstant(Expression exp)
        {
            return (exp.NodeType == ExpressionType.Constant && ((ConstantExpression)exp).Value == null);
        }

        public static string TranslateExpression(Expression e, string partitionKeyProperty, string rowKeyProperty, EntityPropertyConverterOptions options)
        {
            var translator = new AzureStorageQueryTranslator(partitionKeyProperty, rowKeyProperty);
            translator._options = options;
            translator.Visit(e);

            return translator.Filter.ToString();
        }

        protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding node)
        {
            return base.VisitMemberMemberBinding(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Should perhaps add evaluation for right side values, but this requires a bit of
            // wrangling since VisitMember/VisitConstant is going to be run first.
            // Might need to start collecting a node graph for this purpose, and then at the end
            // serialize the node graph. We'd effectively need to erase the last expression filter output
            // when we arrive here as this replaces that output.

            // Also need to add a special case for AsComparable() so that when that is used,
            // we skip this and continue as if there was no method call.
            //var lambda = Expression.Lambda(node, _nodeParameters);
            //var getter = lambda.Compile();
            //getter.
            
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            var expression = node.Body;
            Visit(expression);

            return node;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            return base.VisitNew(node);
        }

        protected override Expression VisitMember(MemberExpression m)
        {

            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter && _binarySideStack.Peek() != BinarySide.Right)
            {
                if (_memberDepth == 0 && m.Member.Name == _partitionKeyProperty)
                    Filter.Append("PartitionKey");
                else if (_memberDepth == 0 && m.Member.Name == _rowKeyProperty)
                    Filter.Append("RowKey");
                else
                    Filter.Append(m.Member.Name);
                _lastVisitedMemberExpression = m;
                return m;
            }
            if (m.Expression != null && m.Expression.NodeType == ExpressionType.MemberAccess && _binarySideStack.Peek() != BinarySide.Right)
            {
                _memberDepth++;
                MemberExpression innerExpression = m.Expression as MemberExpression;
                VisitMember(innerExpression);
                Filter.Append(_options.PropertyNameDelimiter + m.Member.Name);
                _memberDepth--;
                return m;
            }

            if (_binarySideStack.Peek() == BinarySide.Right)
            {
                var value = GetValue(m);
                Filter.Append(ValueAsString(value));
                return m;
            }

            throw new NotSupportedException(string.Format("The member '{0}' is not supported", m.Member.Name));
        }

        private string ValueAsString(object value)
        {
            if (value == null)
                return "NULL";

            var ic = CultureInfo.InvariantCulture;
            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.DateTime:
                    return $"datetime'{((DateTime)value):o}'";
                case TypeCode.String:
                    return $"'{value.ToString().Replace("'", "''")}'";
                case TypeCode.Boolean:
                    return (bool) value ? "true" : "false";
                case TypeCode.Decimal:
                    return ((decimal) value).ToString(ic);
                case TypeCode.Double:
                    return ((double) value).ToString(ic);
                case TypeCode.Single:
                    return ((float) value).ToString(ic);
                case TypeCode.Int16:
                    return ((short) value).ToString(ic);
                case TypeCode.Int32:
                    return ((int) value).ToString(ic);
                case TypeCode.Int64:
                    return ((long)value).ToString(ic);
                case TypeCode.UInt16:
                    return ((ushort)value).ToString(ic);
                case TypeCode.UInt32:
                    return ((uint)value).ToString(ic);
                case TypeCode.UInt64:
                    return ((ulong)value).ToString(ic);
                case TypeCode.Object:
                    throw new NotImplementedException("Unsupported type: Object conversion to string for queries not supported");
            }

            return value.ToString();
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            var value = c.Value;
            if (value == null)
            {
                Filter.Append("NULL");
            }
            else
            {
                Filter.Append(ValueAsString(value));
            }

            return c;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {

            if (node.NodeType == ExpressionType.Not)
            {
                Visit(node.Operand);
                Filter.Append(" not true");
                return node.Operand;
            }
            
            return base.VisitUnary(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {

            if (node.Type == typeof(DateTimeOffset) && _binarySideStack.Peek() == BinarySide.Left)
                Filter.Append("Timestamp");

            return base.VisitParameter(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {

            Filter.Append("(");

            _binarySideStack.Push(BinarySide.Left);
            Visit(node.Left);
            _binarySideStack.Pop();

            string op = "";

            switch (node.NodeType)
            {
                case ExpressionType.And:
                    op = "and";
                    break;

                case ExpressionType.AndAlso:
                    op = "and";
                    break;

                case ExpressionType.Or:
                    op = "or";
                    break;

                case ExpressionType.OrElse:
                    op = "or";
                    break;

                case ExpressionType.Equal:
                    op = "eq";
                    break;

                case ExpressionType.NotEqual:
                    op = "not";
                    break;

                case ExpressionType.LessThan:
                    op = "lt";
                    break;

                case ExpressionType.LessThanOrEqual:
                    op = "lte";
                    break;

                case ExpressionType.GreaterThan:
                    op = "gt";
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    op = "gte";
                    break;

                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", node.NodeType));
            }

            Filter.Append($" {op} ");

            _binarySideStack.Push(BinarySide.Right);
            Visit(node.Right);
            _binarySideStack.Pop();

            Filter.Append(")");

            return node;

        }


        private object GetValue(MemberExpression member)
        {
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

    }
}