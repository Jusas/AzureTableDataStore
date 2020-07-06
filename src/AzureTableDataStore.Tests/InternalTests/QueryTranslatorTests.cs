using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using AzureTableDataStore.Tests.Models;
using FluentAssertions;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace AzureTableDataStore.Tests.InternalTests
{
    public class QueryTranslatorTests
    {

        private UserProfile _sourceObject;
        private EntityPropertyConverterOptions _entityPropertyConverterOptions = new EntityPropertyConverterOptions();
        public QueryTranslatorTests()
        {
            _sourceObject = new UserProfile()
            {
                Age = 55,
                Name = "James Bond",
                Aliases = new List<string>() { "Cmdr Bond", "James", "007" },
                ExtendedProperties = new UserProfile.ProfileProperties()
                {
                    AverageVisitLengthSeconds = 5,
                    HasVisitedBefore = true
                },
                UserType = "agent",
                UserId = "007",
                ProfileImagery = new UserProfile.ProfileImages()
                {
                    Current = new LargeBlob("bond_new.png", new MemoryStream()),
                    Old = new LargeBlob("bond_old.png", new MemoryStream()),
                }
            };
        }



        [Fact]
        public void Should_handle_constants()
        {

            var expressions = new Expression<Func<UserProfile, bool>>[]
            {
                x => x.UserId == "11",
                x => x.ExtendedProperties.HasVisitedBefore == true,
                x => x.ProfileImagery.Old.Filename == "meh"
            };

            var expectedResults = new string[]
            {
                "(UserId eq '11')",
                "(ExtendedProperties_HasVisitedBefore eq true)",
                "(ProfileImagery_Old_Filename eq 'meh')"
            };


            for (var i = 0; i < expressions.Length; i++)
            {
                var translated = AzureStorageQueryTranslator
                    .TranslateExpression(expressions[i], "", "", _entityPropertyConverterOptions);
                translated.Should().Be(expectedResults[i]);
            }

        }

        [Fact]
        public void Should_handle_members()
        {

            var expressions = new Expression<Func<UserProfile, bool>>[]
            {
                x => x.UserId == _sourceObject.UserId,
                x => x.ExtendedProperties.HasVisitedBefore == _sourceObject.ExtendedProperties.HasVisitedBefore,
                x => x.ProfileImagery.Old.Filename == _sourceObject.ProfileImagery.Old.Filename
            };

            var expectedResults = new string[]
            {
                "(UserId eq '007')",
                "(ExtendedProperties_HasVisitedBefore eq true)",
                "(ProfileImagery_Old_Filename eq 'bond_old.png')"
            };


            for (var i = 0; i < expressions.Length; i++)
            {
                var translated = AzureStorageQueryTranslator
                    .TranslateExpression(expressions[i], "", "", _entityPropertyConverterOptions);
                translated.Should().Be(expectedResults[i]);
            }

        }

        [Fact]
        public void Should_handle_combined_expressions()
        {

            var expressions = new Expression<Func<UserProfile, bool>>[]
            {
                x => x.UserId == _sourceObject.UserId && x.Name == "James",
                x => x.ExtendedProperties.HasVisitedBefore == _sourceObject.ExtendedProperties.HasVisitedBefore || x.Name == _sourceObject.Name,
                x => (x.UserId == "Test" && x.Name == "James") || (x.UserId == "1" && x.Name == _sourceObject.Name)
            };

            var expectedResults = new string[]
            {
                "((UserId eq '007') and (Name eq 'James'))",
                "((ExtendedProperties_HasVisitedBefore eq true) or (Name eq 'James Bond'))",
                "(((UserId eq 'Test') and (Name eq 'James')) or ((UserId eq '1') and (Name eq 'James Bond')))"
            };


            for (var i = 0; i < expressions.Length; i++)
            {
                var translated = AzureStorageQueryTranslator
                    .TranslateExpression(expressions[i], "", "", _entityPropertyConverterOptions);
                translated.Should().Be(expectedResults[i]);
            }

        }

        [Fact]
        public void Should_translate_correct_operators()
        {

            var expressions = new Expression<Func<UserProfile, bool>>[]
            {
                x => x.UserId == _sourceObject.UserId,
                x => x.Age > 1,
                x => x.Age < 1,
                x => x.Age >= 1,
                x => x.Age <= 1,
                x => x.Age != 1,
                x => x.Name.AsComparable() > _sourceObject.Name.AsComparable(),
                x => !x.ExtendedProperties.HasVisitedBefore,
                x => x.ExtendedProperties.HasVisitedBefore == true
            };

            var expectedResults = new string[]
            {
                "(UserId eq '007')",
                "(Age gt 1)",
                "(Age lt 1)",
                "(Age gte 1)",
                "(Age lte 1)",
                "(Age not 1)",
                "(Name gt 'James Bond')",
                "ExtendedProperties_HasVisitedBefore not true",
                "(ExtendedProperties_HasVisitedBefore eq true)"
            };

            for (var i = 0; i < expressions.Length; i++)
            {
                var translated = AzureStorageQueryTranslator
                    .TranslateExpression(expressions[i], "", "", _entityPropertyConverterOptions);
                translated.Should().Be(expectedResults[i]);
            }

        }

        [Fact]
        public void Should_translate_select_expression()
        {

            Expression<Func<UserProfile, object>> expression = profile =>
                new {profile.Name, profile.Age, profile.ExtendedProperties.HasVisitedBefore};

            var expectedResults = new List<string>()
            {
                "Name",
                "Age",
                "ExtendedProperties_HasVisitedBefore"
            };

            var translated = AzureStorageQuerySelectTranslator.TranslateExpressionToMemberNames(expression,
                _entityPropertyConverterOptions);

            for (var i = 0; i < translated.Count; i++)
            {
                translated[i].Should().Be(expectedResults[i]);
            }
        }        
        
        [Fact]
        public void Should_translate_deeply_nested_select_expression()
        {

            Expression<Func<DeepStructuredObject, object>> expression = d =>
                new {d.Level0.Level1.Level2.FinalProperty};

            var expectedResults = new List<string>()
            {
                "Level0_Level1_Level2_FinalProperty"
            };

            var translated = AzureStorageQuerySelectTranslator.TranslateExpressionToMemberNames(expression,
                _entityPropertyConverterOptions);

            for (var i = 0; i < translated.Count; i++)
            {
                translated[i].Should().Be(expectedResults[i]);
            }
        }

        [Fact]
        public void Should_translate_timestamp_expression_properly()
        {

            Expression<Func<SecretWeapon, DateTime, bool>>
                expression = (x, dt) => dt > DateTime.Parse("2020-01-01T00:00:00.000Z");

            var translated = AzureStorageQueryTranslator.TranslateExpression(expression, 
                "", "", _entityPropertyConverterOptions);


        }
    }
}