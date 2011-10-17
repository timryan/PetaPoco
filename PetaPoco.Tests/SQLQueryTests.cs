using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using PetaPoco;
using PetaTest;

namespace PetaPoco.Tests
{

    public class Customer
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public string Address { get; set; }
        public string Phone { get; set; }
    }

    public class Product
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public DateTime ReleaseDate { get; set; }
        public double Price { get; set; }
    }

    public class Order
    {
        public int ID { get; set; }
        public int CustomerID { get; set; }
        public DateTime OrderDate { get; set; }
    }

    public class OrderItem
    {
        public int ID { get; set; }
        public int OrderID { get; set; }
        public int ProductID { get; set; }
        public int Quanity { get; set; }
        public double Price { get; set; }

        [PetaPoco.Ignore]
        public double Total
        {
            get
            {
                return Price * Quanity;
            }
        }
    }

    [TestFixture]
    public class SQLQueryTests
    {

        [Test]
        public void From()
        {
            string sql = new SQLQuery<Product>()
                        .Build()
                        .SQL;
            Assert.IsTrue(sql.Contains("FROM [Product]"));
        }

        [Test]
        public void InnerJoin()
        {
            var query = new SQLQuery<Order>()
                .From
                    .InnerJoin<Customer>(x => x.CustomerID, x => x.ID)
                .Return();

            QueryStatement statement = query.Build();
        }


        [Test]
        public void LeftJoin()
        {
            var query = new SQLQuery<Order>()
                .From.InnerJoin<Customer>(x => x.CustomerID, x => x.ID, false)
                    .LeftOuterJoin<OrderItem>(x => x.ID, x => x.OrderID)
                    .LeftOuterJoin<OrderItem, Product, int>(x => x.ProductID, x => x.ID)
                .Return();

            QueryStatement statement = query.Build();

            Assert.IsTrue(statement.SQL.Contains("LEFT OUTER JOIN"));
        }

        [Test]
        public void Where()
        {
            var query = new SQLQuery<Order>()
                .From
                    .InnerJoin<Customer>(x => x.CustomerID, x => x.ID)
                    .LeftOuterJoin<OrderItem>(x => x.ID, x => x.OrderID, false)
                .Where.Equal<Customer>(x => x.Name, "Tim")
                    .Or
                    .START
                        .Equal<Customer, string>(x => x.Name, "Julia")
                        .And
                        .Equal(x => x.ID, 5)
                    .END
                .Return();

            QueryStatement statement = query.Build();

            Assert.AreEqual(3, statement.Parameters.Length);
        }

        [Test]
        public void OrderBy()
        {
            var query = new SQLQuery<Order>()
                .From
                    .InnerJoin<Customer>(x => x.CustomerID, x => x.ID)
                    .LeftOuterJoin<OrderItem>(x => x.ID, x => x.OrderID, false)
                .OrderBy.Ascending<Customer>(x => x.Name)
                    .Descending(x => x.OrderDate)
                .Return();

            string sql = query.Build().SQL;
            Assert.IsTrue(sql.Contains("b.[Name]"));
            Assert.IsTrue(sql.Contains("a.[OrderDate] DESC"));
        }

        [Test]
        public void Select()
        {
            var query = new SQLQuery<Order>()
                .Select.Add(x => x.CustomerID).Add(x => x.OrderDate)
                .Return();

            string sql = query.Build().SQL;
            Assert.IsTrue(sql.Contains("a.[CustomerID]"));
            Assert.IsTrue(sql.Contains("a.[OrderDate]"));
        }

    }
}
