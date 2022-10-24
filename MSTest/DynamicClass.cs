using AspectCore.DynamicProxy;
using Castle.DynamicProxy;
using KingAOP;
using KingAOP.Aspects;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using IInterceptor = Castle.DynamicProxy.IInterceptor;

namespace MSTest
{
    public class DynamicClass
    {

    }

    #region Castle.net
    public interface SomeInterface
    {

        void DoSomeA();
        void DoSome();
    }
    public class ImpClass : SomeInterface
    {
        private string _s;

        public ImpClass(string s = "ssss")
        {
            this._s = s;
        }

        public void DoSomeA()
        {
            Console.WriteLine("123");
        }

        public virtual void DoSome()
        {
            Console.WriteLine(_s);
        }
    }
    public class SomeInterceptor : IInterceptor
    {
        public void Intercept(IInvocation invocation)
        {
            //Do before...
            System.Console.WriteLine("Do before...");
            try
            {
                invocation.Proceed();
            }
            catch (Exception ex)
            {
                //...
                System.Console.WriteLine(ex.Message);
            }
            //Do after...
            System.Console.WriteLine("Do after...");
        }
    }
    #endregion

    #region System.Reflection.DispatchProxy
    //需要被生成代理实例的接口
    public interface targetInterface
    {
        //这个方法会被代理类实现
        void Write(string writesomeshing);

        void WriteTwo(string writesomeshing);
    }

    public class SampleProxy : DispatchProxy
    {
        /// <summary>
        /// 拦截调用
        /// </summary>
        /// <param name="method">所拦截的方法信息</param>
        /// <param name="parameters">所拦截方法被传入的参数指</param>
        /// <returns></returns>
        protected override object Invoke(MethodInfo targetMethod, object[] args)
        {
            Console.WriteLine(args[0]);
            return null;
        }
    }
    #endregion

    #region KingAOP
    public class AopFilter : OnMethodBoundaryAspect
    {
        public override void OnEntry(MethodExecutionArgs args)
        {
            Console.WriteLine("call-------->AopFilter------>OnEntry");
            base.OnEntry(args);
        }

        public override void OnException(MethodExecutionArgs args)
        {
            Console.WriteLine("call-------->AopFilter------>OnException");
            base.OnException(args);
        }

        public override void OnExit(MethodExecutionArgs args)
        {
            Console.WriteLine("call-------->AopFilter------>OnExit");
            base.OnExit(args);
        }

        public override void OnSuccess(MethodExecutionArgs args)
        {
            Console.WriteLine("call-------->AopFilter------>OnSuccess");
            base.OnSuccess(args);
        }
    }
    public class SimonDemo : IDynamicMetaObjectProvider
    {

        public SimonDemo()
        {
            Console.WriteLine(" Call 'SimonDemo类' - 'Constructor(构造函数)'");
        }

        public DynamicMetaObject GetMetaObject(Expression parameter)
        {
            return new AspectWeaver(parameter, this);
        }

        [AopFilter]
        public void Operate()
        {
            Console.WriteLine("Call 'SimonDemo类' - 'Operate方法' ");
        }
    }
    #endregion
}
