using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace AzureTableDataStore
{
    // Old code, probably useless. TODO remove if useless
    internal static class ExpressionExtensions
    {

        public static IEnumerable<object> ExtractConstants(this Expression expression)
        {
            if (expression == null || expression is ParameterExpression)
                return new object[0];

            if (expression is MemberExpression memberExpression)
                return ExtractConstants(memberExpression);

            if (expression is ConstantExpression constantExpression)
                return ExtractConstants(constantExpression);

            if (expression is NewArrayExpression newArrayExpression)
                return ExtractConstants(newArrayExpression);

            if (expression is NewExpression newExpression)
                return ExtractConstants(newExpression);

            if (expression is UnaryExpression unaryExpression)
                return ExtractConstants(unaryExpression);

            if (expression is MethodCallExpression methodCallExpression)
                return ExtractConstants(methodCallExpression);

            return new object[0];
        }

        private static IEnumerable<object> ExtractConstants(MethodCallExpression methodCallExpression)
        {
            var constants = new List<object>();
            foreach (var arg in methodCallExpression.Arguments)
            {
                constants.AddRange(ExtractConstants(arg));
            }

            constants.AddRange(ExtractConstants(methodCallExpression.Object));

            return constants;
        }

        private static IEnumerable<object> ExtractConstants(UnaryExpression unaryExpression)
        {
            return ExtractConstants(unaryExpression.Operand);
        }

        private static IEnumerable<object> ExtractConstants(NewExpression newExpression)
        {
            var arguments = new List<object>();
            foreach (var argumentExpression in newExpression.Arguments)
            {
                arguments.AddRange(ExtractConstants(argumentExpression));
            }

            yield return newExpression.Constructor.Invoke(arguments.ToArray());
        }

        private static IEnumerable<object> ExtractConstants(NewArrayExpression newArrayExpression)
        {
            Type type = newArrayExpression.Type.GetElementType();
            if (type is IConvertible)
                return ExtractConvertibleTypeArrayConstants(newArrayExpression, type);

            return ExtractNonConvertibleArrayConstants(newArrayExpression, type);
        }

        private static IEnumerable<object> ExtractNonConvertibleArrayConstants(NewArrayExpression newArrayExpression, Type type)
        {
            var arrayElements = CreateList(type);
            foreach (var arrayElementExpression in newArrayExpression.Expressions)
            {
                object arrayElement;

                if (arrayElementExpression is ConstantExpression)
                    arrayElement = ((ConstantExpression)arrayElementExpression).Value;
                else
                    arrayElement = ExtractConstants(arrayElementExpression).ToArray();

                if (arrayElement is object[])
                {
                    foreach (var item in (object[])arrayElement)
                        arrayElements.Add(item);
                }
                else
                    arrayElements.Add(arrayElement);
            }

            return ToArray(arrayElements);
        }

        private static IEnumerable<object> ToArray(IList list)
        {
            var toArrayMethod = list.GetType().GetMethod("ToArray");
            yield return toArrayMethod.Invoke(list, new Type[] { });
        }

        private static IList CreateList(Type type)
        {
            //return (IList)typeof(List<>).MakeGenericType(type).GetConstructor(new Type[0]).Invoke(BindingFlags.CreateInstance, null, null, null);
            return (IList)typeof(List<>).MakeGenericType(type).GetConstructor(new Type[0]).Invoke(new object[0]);
        }

        private static IEnumerable<object> ExtractConvertibleTypeArrayConstants(NewArrayExpression newArrayExpression, Type type)
        {
            var arrayElements = CreateList(type);
            foreach (var arrayElementExpression in newArrayExpression.Expressions)
            {
                var arrayElement = ((ConstantExpression)arrayElementExpression).Value;
                arrayElements.Add(Convert.ChangeType(arrayElement, arrayElementExpression.Type, null));
            }

            yield return ToArray(arrayElements);
        }

        private static IEnumerable<object> ExtractConstants(ConstantExpression constantExpression)
        {
            var constants = new List<object>();

            if (constantExpression.Value is Expression)
            {
                constants.AddRange(ExtractConstants((Expression)constantExpression.Value));
            }
            else
            {
                if (constantExpression.Type == typeof(string) ||
                    constantExpression.Type.GetTypeInfo().IsPrimitive ||
                    constantExpression.Type.GetTypeInfo().IsEnum ||
                    constantExpression.Value == null)
                {
                    constants.Add(constantExpression.Value);
                }
            }

            return constants;
        }

        private static IEnumerable<object> ExtractConstants(MemberExpression memberExpression)
        {
            var constants = new List<object>();
            var constExpression = memberExpression.Expression as ConstantExpression;
            var valIsConstant = constExpression != null;
            Type declaringType = memberExpression.Member.DeclaringType;
            object declaringObject = memberExpression.Member;

            if (valIsConstant)
            {
                declaringType = constExpression.Type;
                declaringObject = constExpression.Value;
            }

            var member = declaringType.GetTypeInfo().GetMember(memberExpression.Member.Name, MemberTypes.Field | MemberTypes.Property, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static).Single();

            if (member.MemberType == MemberTypes.Field)
                constants.Add(((FieldInfo)member).GetValue(declaringObject));
            else
                constants.Add(((PropertyInfo)member).GetGetMethod(true).Invoke(declaringObject, null));

            return constants;
        }
    }
}
