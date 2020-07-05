using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableDataStore
{
    internal class ReflectionUtils
    {

        public abstract class PropertyRef
        {
            public object SourceObject { get; set; }
            public Type SourceObjectType { get; set; }
            public PropertyInfo Property { get; set; }
            public string FlattenedPropertyName { get; set; }
            public abstract object StoredInstanceAsObject { get; }
        }

        public class PropertyRef<T> : PropertyRef
        {
            public T StoredInstance { get; set; }

            public override object StoredInstanceAsObject => StoredInstance;
        }


        public static List<PropertyRef<ICollection>> GatherPropertiesWithCollectionsRecursive(
            object obj, EntityPropertyConverterOptions opts,
            List<string> propertyPath = null, List<PropertyRef<ICollection>> collectedCollRefs = null, bool includeNulls = false)
        {
            if (propertyPath == null)
                propertyPath = new List<string>();

            if(collectedCollRefs == null)
                collectedCollRefs = new List<PropertyRef<ICollection>>();

            if (obj == null)
                return collectedCollRefs;
            
            var objType = obj.GetType();
            var properties = objType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var collectionProps = properties.Where(x => 
                typeof(IDictionary).IsAssignableFrom(x.PropertyType) ||
                typeof(IList).IsAssignableFrom(x.PropertyType));

            var thisObjCollPropRefs = collectionProps.Select(x => new PropertyRef<ICollection>()
            {
                StoredInstance = (ICollection) x.GetValue(obj),
                FlattenedPropertyName = string.Join(opts.PropertyNameDelimiter, propertyPath.Append(x.Name)),
                SourceObject = obj,
                SourceObjectType = objType,
                Property = x
            }).Where(x => includeNulls || x.StoredInstance != null);

            collectedCollRefs.AddRange(thisObjCollPropRefs);

            var otherProperties = properties.Where(x => !IsCollectionProperty(x) && !IsBlobRefProperty(x) && x.PropertyType.IsClass);
            foreach (var property in otherProperties)
            {
                var innerPropertyPath = new List<string>(propertyPath);
                innerPropertyPath.Add(property.Name);
                var propertyValue = property.GetValue(obj);
                if (propertyValue != null)
                    GatherPropertiesWithCollectionsRecursive(propertyValue, opts, innerPropertyPath, collectedCollRefs);
                else if(includeNulls)
                    GatherPropertiesWithCollectionsRecursive(property.PropertyType, opts, innerPropertyPath, collectedCollRefs);
            }

            return collectedCollRefs;

        }

        public static List<PropertyRef<ICollection>> GatherPropertiesWithCollectionsRecursive(Type type,
            EntityPropertyConverterOptions opts, List<string> propertyPath = null,
            List<PropertyRef<ICollection>> collectedCollRefs = null)
        {
            if (propertyPath == null)
                propertyPath = new List<string>();

            if (collectedCollRefs == null)
                collectedCollRefs = new List<PropertyRef<ICollection>>();
            
            var objType = type;
            var properties = objType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var collectionProps = properties.Where(x =>
                typeof(IDictionary).IsAssignableFrom(x.PropertyType) ||
                typeof(IList).IsAssignableFrom(x.PropertyType));

            var thisObjCollPropRefs = collectionProps.Select(x => new PropertyRef<ICollection>()
            {
                StoredInstance = null,
                FlattenedPropertyName = string.Join(opts.PropertyNameDelimiter, propertyPath.Append(x.Name)),
                SourceObject = null,
                SourceObjectType = objType,
                Property = x
            });

            collectedCollRefs.AddRange(thisObjCollPropRefs);

            var otherProperties = properties.Where(x => !IsCollectionProperty(x) && !IsBlobRefProperty(x) && x.PropertyType.IsClass);
            foreach (var property in otherProperties)
            {
                var innerPropertyPath = new List<string>(propertyPath);
                innerPropertyPath.Add(property.Name);
                GatherPropertiesWithCollectionsRecursive(property.PropertyType, opts, innerPropertyPath, collectedCollRefs);
            }

            return collectedCollRefs;
        }

        private static bool IsCollectionProperty(PropertyInfo propertyInfo) 
            => typeof(IDictionary).IsAssignableFrom(propertyInfo.PropertyType) ||
               typeof(IList).IsAssignableFrom(propertyInfo.PropertyType);

        private static bool IsBlobRefProperty(PropertyInfo propertyInfo)
            => propertyInfo.PropertyType == typeof(LargeBlob);


        public static List<PropertyRef<LargeBlob>> GatherPropertiesWithBlobsRecursive(object obj, EntityPropertyConverterOptions opts, 
            List<string> propertyPath = null, List<PropertyRef<LargeBlob>> collectedBlobRefs = null, bool includeNulls = false)
        {
            
            if(propertyPath == null)
                propertyPath = new List<string>();

            if(collectedBlobRefs == null)
                collectedBlobRefs = new List<PropertyRef<LargeBlob>>();

            if (obj == null)
                return collectedBlobRefs;

            var objType = obj.GetType();
            var properties = objType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var blobProps = properties.Where(IsBlobRefProperty);

            var thisObjBlobPropRefs = blobProps.Select(x => new PropertyRef<LargeBlob>()
            {
                StoredInstance = (LargeBlob)x.GetValue(obj),
                FlattenedPropertyName = string.Join(opts.PropertyNameDelimiter, propertyPath.Append(x.Name)),
                SourceObject = obj,
                SourceObjectType = objType,
                Property = x
            }).Where(x => includeNulls || x.StoredInstance != null);

            collectedBlobRefs.AddRange(thisObjBlobPropRefs);

            var otherProperties = properties.Where(x => !IsBlobRefProperty(x) && !IsCollectionProperty(x) && x.PropertyType.IsClass);
            foreach (var property in otherProperties)
            {
                var innerPropertyPath = new List<string>(propertyPath);
                innerPropertyPath.Add(property.Name);
                var propertyValue = property.GetValue(obj);
                if(propertyValue != null)
                    GatherPropertiesWithBlobsRecursive(propertyValue, opts, innerPropertyPath, collectedBlobRefs);
                else if(includeNulls)
                    GatherPropertiesWithBlobsRecursive(property.PropertyType, opts, innerPropertyPath, collectedBlobRefs);
            }

            return collectedBlobRefs;
        }

        public static List<PropertyRef<LargeBlob>> GatherPropertiesWithBlobsRecursive(Type type, EntityPropertyConverterOptions opts,
            List<string> propertyPath = null, List<PropertyRef<LargeBlob>> collectedBlobRefs = null)
        {

            if (propertyPath == null)
                propertyPath = new List<string>();

            if (collectedBlobRefs == null)
                collectedBlobRefs = new List<PropertyRef<LargeBlob>>();

            var objType = type;
            var properties = objType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            var blobProps = properties.Where(IsBlobRefProperty);

            var thisObjBlobPropRefs = blobProps.Select(x => new PropertyRef<LargeBlob>()
            {
                StoredInstance = null,
                FlattenedPropertyName = string.Join(opts.PropertyNameDelimiter, propertyPath.Append(x.Name)),
                SourceObject = null,
                SourceObjectType = objType,
                Property = x
            });

            collectedBlobRefs.AddRange(thisObjBlobPropRefs);

            var otherProperties = properties.Where(x => !IsBlobRefProperty(x) && !IsCollectionProperty(x) && x.PropertyType.IsClass);
            foreach (var property in otherProperties)
            {
                var innerPropertyPath = new List<string>(propertyPath);
                innerPropertyPath.Add(property.Name);
                GatherPropertiesWithBlobsRecursive(property.PropertyType, opts, innerPropertyPath, collectedBlobRefs);
            }

            return collectedBlobRefs;
        }
    }
}