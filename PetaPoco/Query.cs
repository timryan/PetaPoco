using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Configuration;
using System.Reflection;

namespace PetaPoco
{
    public abstract class Clause<T>
    {

        protected SQLQuery<T> query;

        public Clause(SQLQuery<T> query)
        {
            this.query = query;
        }

        public QueryStatement Build()
        {
            return query.Build();
        }

        public SQLQuery<T> Return()
        {
            return query;
        }

        public T Single()
        {
            QueryStatement statement = this.Build();
            return query.DB.SingleOrDefault<T>(statement.SQL, statement.Parameters);
        }

        public IEnumerable<T> Query()
        {
            QueryStatement statement = this.Build();
            return query.DB.Query<T>(statement.SQL, statement.Parameters);
        }

        public IEnumerable<T> Fetch()
        {
            QueryStatement statement = this.Build();
            return query.DB.Fetch<T>(statement.SQL, statement.Parameters);
        }
    }

    public static class SpecialQueries
    {
        public static bool TableExists<T>(this Database db)
        {
            IDBProvider dbProvider = GetDBProvider(db);
            Database.PocoData pocoData = Database.PocoData.ForType(typeof(T));
            int result = db.ExecuteScalar<int>(dbProvider.TableExists(pocoData.TableInfo.TableName));
            return result > 0;
        }

        public static IDBProvider GetDBProvider(this Database db)
        {
            return new SQLServerProvider();
        }
    }

    public class EqualityExpression<T, PropType> : EqualityExpression
    {

        public EqualityExpression(string column, PropType value) :
            this(column, ComparisonType.Equal, value)
        {

        }

        public EqualityExpression(string column, ComparisonType comparisonType, PropType value)
        {
            Value = value;
            ComparisonType = ComparisonType.Equal;
            PocoData = Database.PocoData.ForType(typeof(T));
            Column = PocoData.Columns[column];
        }

    }

    public abstract class EqualityExpression
    {
        public Database.PocoData PocoData { get; protected set; }
        public Database.PocoColumn Column { get; protected set; }
        public ComparisonType ComparisonType { get; protected set; }
        public object Value { get; protected set; }

    }

    public abstract class ExpressionVisitor
    {
        protected ExpressionVisitor()
        {
        }

        protected virtual Expression Visit(Expression exp)
        {
            if (exp == null)
                return exp;
            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                    return this.VisitUnary((UnaryExpression)exp);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                    return this.VisitBinary((BinaryExpression)exp);
                case ExpressionType.TypeIs:
                    return this.VisitTypeIs((TypeBinaryExpression)exp);
                case ExpressionType.Conditional:
                    return this.VisitConditional((ConditionalExpression)exp);
                case ExpressionType.Constant:
                    return this.VisitConstant((ConstantExpression)exp);
                case ExpressionType.Parameter:
                    return this.VisitParameter((ParameterExpression)exp);
                case ExpressionType.MemberAccess:
                    return this.VisitMemberAccess((MemberExpression)exp);
                case ExpressionType.Call:
                    return this.VisitMethodCall((MethodCallExpression)exp);
                case ExpressionType.Lambda:
                    return this.VisitLambda((LambdaExpression)exp);
                case ExpressionType.New:
                    return this.VisitNew((NewExpression)exp);
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    return this.VisitNewArray((NewArrayExpression)exp);
                case ExpressionType.Invoke:
                    return this.VisitInvocation((InvocationExpression)exp);
                case ExpressionType.MemberInit:
                    return this.VisitMemberInit((MemberInitExpression)exp);
                case ExpressionType.ListInit:
                    return this.VisitListInit((ListInitExpression)exp);
                default:
                    throw new Exception(string.Format("Unhandled expression type: '{0}'", exp.NodeType));
            }
        }

        protected virtual MemberBinding VisitBinding(MemberBinding binding)
        {
            switch (binding.BindingType)
            {
                case MemberBindingType.Assignment:
                    return this.VisitMemberAssignment((MemberAssignment)binding);
                case MemberBindingType.MemberBinding:
                    return this.VisitMemberMemberBinding((MemberMemberBinding)binding);
                case MemberBindingType.ListBinding:
                    return this.VisitMemberListBinding((MemberListBinding)binding);
                default:
                    throw new Exception(string.Format("Unhandled binding type '{0}'", binding.BindingType));
            }
        }

        protected virtual ElementInit VisitElementInitializer(ElementInit initializer)
        {
            ReadOnlyCollection<Expression> arguments = this.VisitExpressionList(initializer.Arguments);
            if (arguments != initializer.Arguments)
            {
                return Expression.ElementInit(initializer.AddMethod, arguments);
            }
            return initializer;
        }

        protected virtual Expression VisitUnary(UnaryExpression u)
        {
            Expression operand = this.Visit(u.Operand);
            if (operand != u.Operand)
            {
                return Expression.MakeUnary(u.NodeType, operand, u.Type, u.Method);
            }
            return u;
        }

        protected virtual Expression VisitBinary(BinaryExpression b)
        {
            Expression left = this.Visit(b.Left);
            Expression right = this.Visit(b.Right);
            Expression conversion = this.Visit(b.Conversion);
            if (left != b.Left || right != b.Right || conversion != b.Conversion)
            {
                if (b.NodeType == ExpressionType.Coalesce && b.Conversion != null)
                    return Expression.Coalesce(left, right, conversion as LambdaExpression);
                else
                    return Expression.MakeBinary(b.NodeType, left, right, b.IsLiftedToNull, b.Method);
            }
            return b;
        }

        protected virtual Expression VisitTypeIs(TypeBinaryExpression b)
        {
            Expression expr = this.Visit(b.Expression);
            if (expr != b.Expression)
            {
                return Expression.TypeIs(expr, b.TypeOperand);
            }
            return b;
        }

        protected virtual Expression VisitConstant(ConstantExpression c)
        {
            return c;
        }

        protected virtual Expression VisitConditional(ConditionalExpression c)
        {
            Expression test = this.Visit(c.Test);
            Expression ifTrue = this.Visit(c.IfTrue);
            Expression ifFalse = this.Visit(c.IfFalse);
            if (test != c.Test || ifTrue != c.IfTrue || ifFalse != c.IfFalse)
            {
                return Expression.Condition(test, ifTrue, ifFalse);
            }
            return c;
        }

        protected virtual Expression VisitParameter(ParameterExpression p)
        {
            return p;
        }

        protected virtual Expression VisitMemberAccess(MemberExpression m)
        {
            Expression exp = this.Visit(m.Expression);
            if (exp != m.Expression)
            {
                return Expression.MakeMemberAccess(exp, m.Member);
            }
            return m;
        }

        protected virtual Expression VisitMethodCall(MethodCallExpression m)
        {
            Expression obj = this.Visit(m.Object);
            IEnumerable<Expression> args = this.VisitExpressionList(m.Arguments);
            if (obj != m.Object || args != m.Arguments)
            {
                return Expression.Call(obj, m.Method, args);
            }
            return m;
        }

        protected virtual ReadOnlyCollection<Expression> VisitExpressionList(ReadOnlyCollection<Expression> original)
        {
            List<Expression> list = null;
            for (int i = 0, n = original.Count; i < n; i++)
            {
                Expression p = this.Visit(original[i]);
                if (list != null)
                {
                    list.Add(p);
                }
                else if (p != original[i])
                {
                    list = new List<Expression>(n);
                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }
                    list.Add(p);
                }
            }
            if (list != null)
            {
                return list.AsReadOnly();
            }
            return original;
        }

        protected virtual MemberAssignment VisitMemberAssignment(MemberAssignment assignment)
        {
            Expression e = this.Visit(assignment.Expression);
            if (e != assignment.Expression)
            {
                return Expression.Bind(assignment.Member, e);
            }
            return assignment;
        }

        protected virtual MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            IEnumerable<MemberBinding> bindings = this.VisitBindingList(binding.Bindings);
            if (bindings != binding.Bindings)
            {
                return Expression.MemberBind(binding.Member, bindings);
            }
            return binding;
        }

        protected virtual MemberListBinding VisitMemberListBinding(MemberListBinding binding)
        {
            IEnumerable<ElementInit> initializers = this.VisitElementInitializerList(binding.Initializers);
            if (initializers != binding.Initializers)
            {
                return Expression.ListBind(binding.Member, initializers);
            }
            return binding;
        }

        protected virtual IEnumerable<MemberBinding> VisitBindingList(ReadOnlyCollection<MemberBinding> original)
        {
            List<MemberBinding> list = null;
            for (int i = 0, n = original.Count; i < n; i++)
            {
                MemberBinding b = this.VisitBinding(original[i]);
                if (list != null)
                {
                    list.Add(b);
                }
                else if (b != original[i])
                {
                    list = new List<MemberBinding>(n);
                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }
                    list.Add(b);
                }
            }
            if (list != null)
                return list;
            return original;
        }

        protected virtual IEnumerable<ElementInit> VisitElementInitializerList(ReadOnlyCollection<ElementInit> original)
        {
            List<ElementInit> list = null;
            for (int i = 0, n = original.Count; i < n; i++)
            {
                ElementInit init = this.VisitElementInitializer(original[i]);
                if (list != null)
                {
                    list.Add(init);
                }
                else if (init != original[i])
                {
                    list = new List<ElementInit>(n);
                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }
                    list.Add(init);
                }
            }
            if (list != null)
                return list;
            return original;
        }

        protected virtual Expression VisitLambda(LambdaExpression lambda)
        {
            Expression body = this.Visit(lambda.Body);
            if (body != lambda.Body)
            {
                return Expression.Lambda(lambda.Type, body, lambda.Parameters);
            }
            return lambda;
        }

        protected virtual NewExpression VisitNew(NewExpression nex)
        {
            IEnumerable<Expression> args = this.VisitExpressionList(nex.Arguments);
            if (args != nex.Arguments)
            {
                if (nex.Members != null)
                    return Expression.New(nex.Constructor, args, nex.Members);
                else
                    return Expression.New(nex.Constructor, args);
            }
            return nex;
        }

        protected virtual Expression VisitMemberInit(MemberInitExpression init)
        {
            NewExpression n = this.VisitNew(init.NewExpression);
            IEnumerable<MemberBinding> bindings = this.VisitBindingList(init.Bindings);
            if (n != init.NewExpression || bindings != init.Bindings)
            {
                return Expression.MemberInit(n, bindings);
            }
            return init;
        }

        protected virtual Expression VisitListInit(ListInitExpression init)
        {
            NewExpression n = this.VisitNew(init.NewExpression);
            IEnumerable<ElementInit> initializers = this.VisitElementInitializerList(init.Initializers);
            if (n != init.NewExpression || initializers != init.Initializers)
            {
                return Expression.ListInit(n, initializers);
            }
            return init;
        }

        protected virtual Expression VisitNewArray(NewArrayExpression na)
        {
            IEnumerable<Expression> exprs = this.VisitExpressionList(na.Expressions);
            if (exprs != na.Expressions)
            {
                if (na.NodeType == ExpressionType.NewArrayInit)
                {
                    return Expression.NewArrayInit(na.Type.GetElementType(), exprs);
                }
                else
                {
                    return Expression.NewArrayBounds(na.Type.GetElementType(), exprs);
                }
            }
            return na;
        }

        protected virtual Expression VisitInvocation(InvocationExpression iv)
        {
            IEnumerable<Expression> args = this.VisitExpressionList(iv.Arguments);
            Expression expr = this.Visit(iv.Expression);
            if (args != iv.Arguments || expr != iv.Expression)
            {
                return Expression.Invoke(expr, args);
            }
            return iv;
        }
    }


    public class FindMemberVisitor : ExpressionVisitor
    {

        private readonly Stack<MemberInfo> _result;

        private FindMemberVisitor()
        {
            _result = new Stack<MemberInfo>();
        }

        public static string FindMember(Expression expression)
        {
            var finder = new FindMemberVisitor();
            finder.Visit(expression);
            return string.Join(".", finder._result.Select(mi => mi.Name).ToArray());
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            _result.Push(m.Member);
            return base.VisitMemberAccess(m);
        }

    }

       public class FromClause<T>: Clause<T>
    {

        List<JoinExpression> items = new List<JoinExpression>();
        Database.PocoData table;

        public FromClause(SQLQuery<T> query): base(query)
        {
            table = Database.PocoData.ForType(typeof(T));
        }

        #region Inner Join

        public FromClause<T> InnerJoin<TRight>(Expression<Func<T, int>> leftKey, Expression<Func<TRight, int>> rightKey)
        {
            return InnerJoin<T, TRight, int>(leftKey, rightKey, true);
        }

        public FromClause<T> InnerJoin<TRight>(Expression<Func<T, int>> leftKey, Expression<Func<TRight, int>> rightKey, bool display)
        {
            return InnerJoin<T, TRight, int>(leftKey, rightKey, display);
        }

        public FromClause<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, int>> leftKey, Expression<Func<TRight, int>> rightKey)
        {
            return InnerJoin<TLeft, TRight, int>(leftKey, rightKey, true);
        }

        public FromClause<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, int>> leftKey, Expression<Func<TRight, int>> rightKey, bool display)
        {
            return InnerJoin<TLeft, TRight, int>(leftKey, rightKey, display);
        }

        public FromClause<T> InnerJoin<TLeft, TRight, TKey>(Expression<Func<TLeft, TKey>> leftKey, Expression<Func<TRight, TKey>> rightKey)
        {
            return InnerJoin<TLeft, TRight, TKey>(leftKey, rightKey, true);
        }

        public FromClause<T> InnerJoin<TLeft, TRight, TKey>(Expression<Func<TLeft, TKey>> leftKey, Expression<Func<TRight, TKey>> rightKey, bool display)
        {
            JoinExpression join = new JoinExpression(JoinType.Inner, typeof(TLeft), typeof(TRight), FindMemberVisitor.FindMember(leftKey), FindMemberVisitor.FindMember(rightKey), display);
            items.Add(join);
            return this;
        }

        #endregion

        #region Left Join

        public FromClause<T> LeftOuterJoin<TRight>(Expression<Func<T, int>> leftKey, Expression<Func<TRight, int>> rightKey)
        {
            return LeftOuterJoin<T, TRight, int>(leftKey, rightKey, true);
        }

        public FromClause<T> LeftOuterJoin<TRight>(Expression<Func<T, int>> leftKey, Expression<Func<TRight, int>> rightKey, bool display)
        {
            return LeftOuterJoin<T, TRight, int>(leftKey, rightKey, display);
        }

        public FromClause<T> LeftOuterJoin<TLeft, TRight>(Expression<Func<TLeft, int>> leftKey, Expression<Func<TRight, int>> rightKey)
        {
            return LeftOuterJoin<TLeft, TRight, int>(leftKey, rightKey, true);
        }

        public FromClause<T> LeftOuterJoin<TLeft, TRight>(Expression<Func<TLeft, int>> leftKey, Expression<Func<TRight, int>> rightKey, bool display)
        {
            return LeftOuterJoin<TLeft, TRight, int>(leftKey, rightKey, display);
        }

        public FromClause<T> LeftOuterJoin<TLeft, TRight, TKey>(Expression<Func<TLeft, TKey>> leftKey, Expression<Func<TRight, TKey>> rightKey)
        {
            JoinExpression join = new JoinExpression(JoinType.LeftOuter, typeof(TLeft), typeof(TRight), FindMemberVisitor.FindMember(leftKey), FindMemberVisitor.FindMember(rightKey), true);
            items.Add(join);
            return this;
        }

        public FromClause<T> LeftOuterJoin<TLeft, TRight, TKey>(Expression<Func<TLeft, TKey>> leftKey, Expression<Func<TRight, TKey>> rightKey, bool display)
        {
            JoinExpression join = new JoinExpression(JoinType.LeftOuter, typeof(TLeft), typeof(TRight), FindMemberVisitor.FindMember(leftKey), FindMemberVisitor.FindMember(rightKey), display);
            items.Add(join);
            return this;
        }

        #endregion

        #region Right Join

        public FromClause<T> RightOuterJoin<TRight>(Expression<Func<T, int>> leftKey, Expression<Func<TRight, int>> rightKey)
        {
            return RightOuterJoin<T, TRight, int>(leftKey, rightKey, true);
        }

        public FromClause<T> RightOuterJoin<TRight>(Expression<Func<T, int>> leftKey, Expression<Func<TRight, int>> rightKey, bool display)
        {
            return RightOuterJoin<T, TRight, int>(leftKey, rightKey, display);
        }

        public FromClause<T> RightOuterJoin<TLeft, TRight>(Expression<Func<TLeft, int>> leftKey, Expression<Func<TRight, int>> rightKey)
        {
            return RightOuterJoin<TLeft, TRight, int>(leftKey, rightKey, true);
        }

        public FromClause<T> RightOuterJoin<TLeft, TRight>(Expression<Func<TLeft, int>> leftKey, Expression<Func<TRight, int>> rightKey, bool display)
        {
            return RightOuterJoin<TLeft, TRight, int>(leftKey, rightKey, display);
        }

        public FromClause<T> RightOuterJoin<TLeft, TRight, TKey>(Expression<Func<TLeft, TKey>> leftKey, Expression<Func<TRight, TKey>> rightKey)
        {
            return RightOuterJoin<TLeft, TRight, TKey>(leftKey, rightKey, true);
        }

        public FromClause<T> RightOuterJoin<TLeft, TRight, TKey>(Expression<Func<TLeft, TKey>> leftKey, Expression<Func<TRight, TKey>> rightKey, bool display)
        {
            JoinExpression join = new JoinExpression(JoinType.RightOuter, typeof(TLeft), typeof(TRight), FindMemberVisitor.FindMember(leftKey), FindMemberVisitor.FindMember(rightKey), display);
            items.Add(join);
            return this;
        }

        #endregion

        internal Database.PocoData Table
        {
            get
            {
                return table;
            }
        }

        internal IEnumerable<JoinExpression> Items
        {
            get
            {
                return items;
            }
        }

        internal string GetAlias(Database.PocoData table)
        {
            if (this.table.TableInfo.TableName == table.TableInfo.TableName)
                return "a";

            foreach (JoinExpression join in Items)
            {
                if (join.RightTable.TableInfo.TableName == table.TableInfo.TableName)
                {
                    if (join.Alias == null)
                        throw new ArgumentException(string.Format("Incorrect join order, {0} used before it has been added", table.TableInfo.TableName));
                    
                    return join.Alias;
                }
            }

            throw new ArgumentException(string.Format("Table {0} has not been added to the FROM clause", table.TableInfo.TableName));
        }

        public WhereClause<T> Where
        {
            get
            {
                return query.Where;
            }
        }

        public OrderByClause<T> OrderBy
        {
            get
            {
                return query.OrderBy;
            }
        }
    }

    public interface IDBProvider
    {
        string Escape(string column);
        string String(string value);
        string Date(DateTime value);
        string TableExists(string tableName);
        string SchemaDirectory { get; }
    }

    internal enum JoinType { Inner, LeftOuter, RightOuter, CrossJoin }

    internal class JoinExpression
    {

        public JoinType JoinType { get; private set; }
        public Database.PocoData LeftTable { get; private set; }
        public Database.PocoData RightTable { get; private set; }
        public string LeftKey { get; private set; }
        public string RightKey { get; private set; }
        internal string Alias { get; set; }
        public bool Display { get; private set; }

        public JoinExpression(JoinType joinType, Type leftTable, Type rightTable, string leftKey, string rightKey, bool display)
        {
            JoinType = joinType;
            LeftTable = Database.PocoData.ForType(leftTable);
            RightTable = Database.PocoData.ForType(rightTable);
            LeftKey = leftKey;
            RightKey = rightKey;
            Display = display;
        }

    }

    public enum LogicalType { AND, OR }

    public enum ComparisonType
    {
        Equal,
        NotEqual,
        LessThan,
        LessThanEqualTo,
        GreaterThan,
        GreaterThanEqualTo,
        Like,
        IsNull,
        IsNotNull
    }

    public class LogicalExpression<T, ReturnType> : LogicalExpression
    {

        protected ReturnType parent;
        protected SQLQuery<T> query;

        public LogicalExpression(SQLQuery<T> query, ReturnType parent)
        {
            this.query = query;
            this.parent = parent;
        }

        public LogicalExpression<T, ReturnType> And
        {
            get
            {
                items.Add(LogicalType.AND);
                return this;
            }
        }

        public LogicalExpression<T, ReturnType> Or
        {
            get
            {
                items.Add(LogicalType.OR);
                return this;
            }
        }

        public LogicalExpression<T, LogicalExpression<T, ReturnType>> START
        {
            get
            {
                LogicalExpression<T, LogicalExpression<T, ReturnType>> newExp = new LogicalExpression<T, LogicalExpression<T, ReturnType>>(query, this);
                items.Add(newExp);
                return newExp;
            }
        }

        public ReturnType END
        {
            get
            {
                return parent;
            }
        }

        public LogicalExpression<T, ReturnType> Equal(Expression<Func<T, object>> property, object value)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.Equal, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> Equal<PropType>(Expression<Func<T, PropType>> property, PropType value)
        {
            EqualityExpression<T, PropType> exp = new EqualityExpression<T, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.Equal, value);
            items.Add(exp);
            return this;
        }


        public LogicalExpression<T, ReturnType> Equal<PocoType, PropType>(Expression<Func<PocoType, PropType>> property, PropType value)
        {
            EqualityExpression<PocoType, PropType> exp = new EqualityExpression<PocoType, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.Equal, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> Equal<PocoType>(Expression<Func<PocoType, object>> property, object value)
        {
            EqualityExpression<PocoType, object> exp = new EqualityExpression<PocoType, object>(FindMemberVisitor.FindMember(property), ComparisonType.Equal, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> NotEqual<PropType>(Expression<Func<T, PropType>> property, PropType value)
        {
            EqualityExpression<T, PropType> exp = new EqualityExpression<T, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.NotEqual, value);
            items.Add(exp);
            return this;
        }


        public LogicalExpression<T, ReturnType> NotEqual<PocoType, PropType>(Expression<Func<PocoType, PropType>> property, PropType value)
        {
            EqualityExpression<PocoType, PropType> exp = new EqualityExpression<PocoType, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.NotEqual, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> NotEqual(Expression<Func<T, object>> property, object value)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.NotEqual, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> LessThan<PropType>(Expression<Func<T, PropType>> property, PropType value)
        {
            EqualityExpression<T, PropType> exp = new EqualityExpression<T, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.LessThan, value);
            items.Add(exp);
            return this;
        }


        public LogicalExpression<T, ReturnType> LessThan<PocoType, PropType>(Expression<Func<PocoType, PropType>> property, PropType value)
        {
            EqualityExpression<PocoType, PropType> exp = new EqualityExpression<PocoType, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.LessThan, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> LessThan(Expression<Func<T, object>> property, object value)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.LessThan, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> LessThanEqualTo<PropType>(Expression<Func<T, PropType>> property, PropType value)
        {
            EqualityExpression<T, PropType> exp = new EqualityExpression<T, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.LessThanEqualTo, value);
            items.Add(exp);
            return this;
        }


        public LogicalExpression<T, ReturnType> LessThanEqualTo<PocoType, PropType>(Expression<Func<PocoType, PropType>> property, PropType value)
        {
            EqualityExpression<PocoType, PropType> exp = new EqualityExpression<PocoType, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.LessThanEqualTo, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> LessThanEqualTo(Expression<Func<T, object>> property, object value)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.LessThanEqualTo, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> GreaterThan<PropType>(Expression<Func<T, PropType>> property, PropType value)
        {
            EqualityExpression<T, PropType> exp = new EqualityExpression<T, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.GreaterThan, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> GreaterThan<PocoType, PropType>(Expression<Func<PocoType, PropType>> property, PropType value)
        {
            EqualityExpression<PocoType, PropType> exp = new EqualityExpression<PocoType, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.GreaterThan, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> GreaterThan(Expression<Func<T, object>> property, object value)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.GreaterThan, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> GreaterThanEqualTo<PropType>(Expression<Func<T, PropType>> property, PropType value)
        {
            EqualityExpression<T, PropType> exp = new EqualityExpression<T, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.GreaterThanEqualTo, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> GreaterThanEqualTo<PocoType, PropType>(Expression<Func<PocoType, PropType>> property, PropType value)
        {
            EqualityExpression<PocoType, PropType> exp = new EqualityExpression<PocoType, PropType>(FindMemberVisitor.FindMember(property), ComparisonType.GreaterThanEqualTo, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> GreaterThanEqualTo(Expression<Func<T, object>> property, object value)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.GreaterThanEqualTo, value);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> IsNull<PocoType>(Expression<Func<PocoType>> property)
        {
            EqualityExpression<PocoType, object> exp = new EqualityExpression<PocoType, object>(FindMemberVisitor.FindMember(property), ComparisonType.IsNull, null);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> IsNull(Expression<Func<T, object>> property)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.IsNull, null);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> IsNotNull<PocoType>(Expression<Func<PocoType>> property)
        {
            EqualityExpression<PocoType, object> exp = new EqualityExpression<PocoType, object>(FindMemberVisitor.FindMember(property), ComparisonType.IsNotNull, null);
            items.Add(exp);
            return this;
        }

        public LogicalExpression<T, ReturnType> IsNotNull(Expression<Func<T, object>> property)
        {
            EqualityExpression<T, object> exp = new EqualityExpression<T, object>(FindMemberVisitor.FindMember(property), ComparisonType.IsNotNull, null);
            items.Add(exp);
            return this;
        }

        public QueryStatement Build()
        {
            return query.Build();
        }

        public OrderByClause<T> OrderBy
        {
            get
            {
                return query.OrderBy;
            }
        }

        public SQLQuery<T> Return()
        {
            return query;
        }

        public T Single()
        {
            QueryStatement statement = this.Build();
            return query.DB.SingleOrDefault<T>(statement.SQL, statement.Parameters);
        }

        public IEnumerable<T> Query()
        {
            QueryStatement statement = this.Build();
            return query.DB.Query<T>(statement.SQL, statement.Parameters);
        }

        public IEnumerable<T> Fetch()
        {
            QueryStatement statement = this.Build();
            return query.DB.Fetch<T>(statement.SQL, statement.Parameters);
        }

    }

    public class LogicalExpression
    {

        protected List<object> items = new List<object>();

        internal IEnumerable<object> Items
        {
            get
            {
                return items;
            }
        }
    }

    public class OrderByClause<T> : Clause<T>
    {

        protected List<OrderByExpression> items = new List<OrderByExpression>();

        public OrderByClause(SQLQuery<T> query)
            : base(query)
        {

        }

        public OrderByClause<T> Ascending(Expression<Func<T, object>> property)
        {
            return Ascending<T>(property);
        }

        public OrderByClause<T> Ascending<TPoco>(Expression<Func<TPoco, object>> property)
        {
            OrderByExpression<TPoco> exp = new OrderByExpression<TPoco>(FindMemberVisitor.FindMember(property), SortOrder.Ascending);
            items.Add(exp);
            return this;
        }

        public OrderByClause<T> Descending(Expression<Func<T, object>> property)
        {
            return Descending<T>(property);
        }

        public OrderByClause<T> Descending<TPoco>(Expression<Func<TPoco, object>> property)
        {
            OrderByExpression<TPoco> exp = new OrderByExpression<TPoco>(FindMemberVisitor.FindMember(property), SortOrder.Descending);
            items.Add(exp);
            return this;
        }

        internal IEnumerable<OrderByExpression> Items
        {
            get
            {
                return items;
            }
        }

    }

    public enum SortOrder { Ascending, Descending }

    public class OrderByExpression<T> : OrderByExpression
    {

        public OrderByExpression(string column)
            : this(column, SortOrder.Ascending)
        {

        }

        public OrderByExpression(string column, SortOrder order)
        {
            PocoData = Database.PocoData.ForType(typeof(T));
            Column = PocoData.Columns[column];
            SortOrder = order;
        }
    }

    public class OrderByExpression
    {

        public Database.PocoData PocoData { get; protected set; }
        public Database.PocoColumn Column { get; protected set; }
        public SortOrder SortOrder { get; protected set; }

    }

    public class SQLQuery<T>
    {

        public Database DB { get; private set; }
        IDBProvider provider;
        SelectClause<T> select;
        FromClause<T> from;
        WhereClause<T> where;
        OrderByClause<T> orderBy;

        public SQLQuery()
            : this(null)
        {

        }

        public SQLQuery(Database db)
        {
            this.DB = db;
            provider = new SQLServerProvider();

            select = new SelectClause<T>(this);
            from = new FromClause<T>(this);
            where = new WhereClause<T>(this);
            orderBy = new OrderByClause<T>(this);
        }

        public SelectClause<T> Select
        {
            get
            {
                return select;
            }
        }

        public FromClause<T> From
        {
            get
            {
                return from;
            }
        }

        public WhereClause<T> Where
        {
            get
            {
                return where;
            }
        }

        public OrderByClause<T> OrderBy
        {
            get
            {
                return orderBy;
            }
        }

        public T Single()
        {
            QueryStatement statement = this.Build();
            return DB.SingleOrDefault<T>(statement.SQL, statement.Parameters);
        }

        public IEnumerable<T> Query()
        {
            QueryStatement statement = this.Build();
            return DB.Query<T>(statement.SQL, statement.Parameters);
        }

        public IEnumerable<T> Fetch()
        {
            QueryStatement statement = this.Build();
            return DB.Fetch<T>(statement.SQL, statement.Parameters);
        }

        #region Build the SQL Statement

        public QueryStatement Build()
        {

            // build the from statement
            StringBuilder sqlFrom = new StringBuilder();
            sqlFrom.AppendLine("FROM " + provider.Escape(from.Table.TableInfo.TableName) + " AS a ");
            int aliasNo = (int)'a';
            foreach (JoinExpression join in from.Items)
            {

                sqlFrom.Append("\t");
                // get the next alias
                aliasNo++;
                char alias = (char)aliasNo;
                join.Alias = alias.ToString();

                // Add the join type
                switch (join.JoinType)
                {
                    case JoinType.Inner:
                        sqlFrom.Append("INNER JOIN ");
                        break;
                    case JoinType.LeftOuter:
                        sqlFrom.Append("LEFT OUTER JOIN ");
                        break;
                    case JoinType.RightOuter:
                        sqlFrom.Append("RIGHT OUTER JOIN ");
                        break;
                    case JoinType.CrossJoin:
                        sqlFrom.Append("CROSS JOIN ");
                        break;
                }

                sqlFrom.Append(provider.Escape(join.RightTable.TableInfo.TableName) + " AS " + alias + " ");
                sqlFrom.Append("ON ");
                sqlFrom.Append(from.GetAlias(join.LeftTable));
                sqlFrom.Append("." + provider.Escape(join.LeftKey));
                sqlFrom.Append(" = ");
                sqlFrom.Append(alias);
                sqlFrom.Append("." + provider.Escape(join.RightKey));
                sqlFrom.AppendLine();
            }

            // build the select statement
            StringBuilder sqlSelect = new StringBuilder();
            sqlSelect.AppendLine("SELECT ");

            // Auto add all columns
            if (select.Items.Count() == 0)
            {
                sqlSelect.AppendLine("\ta." + provider.Escape(from.Table.TableInfo.PrimaryKey) + ",");
                foreach (string column in from.Table.QueryColumns)
                {
                    if (column != from.Table.TableInfo.PrimaryKey)
                    {
                        sqlSelect.Append("\t");
                        sqlSelect.AppendLine("a." + provider.Escape(column) + ",");
                    }
                }
                foreach (JoinExpression join in from.Items)
                {
                    if (join.Display)
                    {
                        sqlSelect.Append("\t");
                        sqlSelect.AppendLine(join.Alias + "." + provider.Escape(join.RightTable.TableInfo.PrimaryKey) + ",");
                        foreach (string column in join.RightTable.QueryColumns)
                        {
                            if (column != join.RightTable.TableInfo.PrimaryKey)
                            {
                                sqlSelect.Append("\t");
                                sqlSelect.AppendLine(join.Alias + "." + provider.Escape(column) + ",");
                            }
                        }
                    }
                }
            }
            else
            {
                foreach (SelectColumn col in select.Items)
                {
                    sqlSelect.Append("\t");
                    string alias = from.GetAlias(col.Table);
                    sqlSelect.AppendLine(alias + "." + provider.Escape(col.Column.ColumnName) + ",");
                }
            }

            // Remove the last 3 chars: ,\r\n to remove comma
            sqlSelect.Length -= 3;
            sqlSelect.AppendLine();

            List<object> parameters = new List<object>();

            // Build where clause
            StringBuilder sqlWhere = new StringBuilder(BuildLogicalExpression(where, parameters));
            if (sqlWhere.Length > 0)
            {
                sqlWhere.Insert(0, "WHERE ");
                sqlWhere.AppendLine();
            }

            // Build the order by clause
            StringBuilder sqlOrderBy = new StringBuilder();
            foreach (OrderByExpression exp in orderBy.Items)
            {
                // Get the alias
                string alias = from.GetAlias(exp.PocoData);
                sqlOrderBy.Append(" ");
                sqlOrderBy.Append(alias + ".");
                sqlOrderBy.Append(provider.Escape(exp.Column.ColumnName));
                if (exp.SortOrder == SortOrder.Descending)
                    sqlOrderBy.Append(" DESC");
                sqlOrderBy.Append(",");
            }

            if (sqlOrderBy.Length > 0)
            {
                // Remove the last column
                sqlOrderBy.Length--;
                sqlOrderBy.Insert(0, "\nORDER BY \r\n\t");
            }


            QueryStatement statement = new QueryStatement();
            statement.SQL = sqlSelect.ToString() + sqlFrom.ToString() + sqlWhere.ToString() + sqlOrderBy.ToString();
            statement.Parameters = parameters.ToArray();

            return statement;
        }

        private string BuildLogicalExpression(LogicalExpression exp, List<object> parameters)
        {
            StringBuilder sql = new StringBuilder();
            if (exp.Items.Count() > 0)
            {
                sql.AppendLine();
                sql.Append("\t( ");
                foreach (object item in exp.Items)
                {
                    if (item is LogicalType)
                    {
                        LogicalType type = (LogicalType)item;
                        sql.Append(type.ToString() + " ");
                    }
                    else if (item is EqualityExpression)
                    {
                        EqualityExpression eqExp = (EqualityExpression)item;
                        string alias = From.GetAlias(eqExp.PocoData);
                        sql.Append(alias + ".");
                        sql.Append(provider.Escape(eqExp.Column.ColumnName));
                        switch (eqExp.ComparisonType)
                        {
                            case ComparisonType.Equal:
                                sql.Append(" = ");
                                break;
                            case ComparisonType.NotEqual:
                                sql.Append(" <> ");
                                break;
                            case ComparisonType.LessThan:
                                sql.Append(" < ");
                                break;
                            case ComparisonType.LessThanEqualTo:
                                sql.Append(" <= ");
                                break;
                            case ComparisonType.GreaterThan:
                                sql.Append(" > ");
                                break;
                            case ComparisonType.GreaterThanEqualTo:
                                sql.Append(" >= ");
                                break;
                            case ComparisonType.Like:
                                sql.Append(" LIKE ");
                                break;
                            case ComparisonType.IsNull:
                                // Don't need to add a value
                                sql.Append(" IS NULL ");
                                continue;
                            case ComparisonType.IsNotNull:
                                // Don't need to add a value
                                sql.Append(" IS NOT NULL ");
                                continue;
                        }

                        if (parameters == null)
                        {
                            // Insert values instead of parameters
                            if (eqExp.Column.PropertyInfo.PropertyType.Name == "String")
                            {
                                sql.Append(provider.String(eqExp.Value.ToString()));
                            }
                            else if (eqExp.Column.PropertyInfo.PropertyType.Name == "DateTime")
                            {
                                DateTime date = (DateTime)eqExp.Value;
                                sql.Append(provider.String(provider.Date(date)));
                            }
                            else
                            {
                                sql.Append(eqExp.Value.ToString());
                            }
                        }
                        else
                        {
                            sql.Append("@" + parameters.Count.ToString());
                            parameters.Add(eqExp.Value);
                        }

                        sql.Append(" ");
                    }
                    else
                    {
                        LogicalExpression logExp = (LogicalExpression)item;
                        sql.Append(BuildLogicalExpression(logExp, parameters));
                    }
                }

                sql.Append(")");
            }
            return sql.ToString();
        }

        #endregion

        public override string ToString()
        {
            QueryStatement statement = Build();
            return statement.SQL + "\nParameters: " + string.Join(", ", statement.Parameters);
        }
    }

    public class QueryStatement
    {
        public string SQL { get; set; }
        public object[] Parameters { get; set; }
    }

    public class SelectClause<T> : Clause<T>
    {
        List<SelectColumn> items = new List<SelectColumn>();
        Database.PocoData table;

        public SelectClause(SQLQuery<T> query)
            : base(query)
        {
            table = Database.PocoData.ForType(typeof(T));
        }

        internal Database.PocoData Table
        {
            get
            {
                return table;
            }
        }

        public SelectClause<T> Add(Expression<Func<T, object>> column)
        {
            string col = FindMemberVisitor.FindMember(column);
            items.Add(new SelectColumn() { Table = table, Column = table.Columns[col] });
            return this;
        }

        public SelectClause<T> Add<PocoType>(Expression<Func<PocoType, object>> column)
        {
            Database.PocoData t = Database.PocoData.ForType(typeof(PocoType));
            string col = FindMemberVisitor.FindMember(column);
            items.Add(new SelectColumn() { Table = t, Column = t.Columns[col] });
            return this;
        }

        internal IEnumerable<SelectColumn> Items
        {
            get
            {
                return items;
            }
        }

        public FromClause<T> From
        {
            get
            {
                return query.From;
            }
        }

        public WhereClause<T> Where
        {
            get
            {
                return query.Where;
            }
        }

        public OrderByClause<T> OrderBy
        {
            get
            {
                return query.OrderBy;
            }
        }
    }

    public class SelectColumn
    {
        public Database.PocoData Table { get; set; }
        public Database.PocoColumn Column { get; set; }
    }


    public class SQLServerProvider : IDBProvider
    {
        public string Escape(string column)
        {
            return "[" + column + "]";
        }

        public string String(string value)
        {
            return "'" + value + "'";
        }

        public string Date(DateTime value)
        {
            return "'" + value.ToString("yyyy-MM-dd") + "'";
        }

        public string TableExists(string tableName)
        {
            return string.Format("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}'", tableName);
        }

        public string SchemaDirectory
        {
            get { return "SQLServer"; }
        }
    }

    public class WhereClause<T> : LogicalExpression<T, SQLQuery<T>>
    {

        public WhereClause(SQLQuery<T> query)
            : base(query, query)
        {

        }

    }

}
