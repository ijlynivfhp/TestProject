using Castle.DynamicProxy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MSTest
{
    public class DynamicClass
    {

    }

    public interface SomeInterface {

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
            catch(Exception ex)
            {
                //...
                System.Console.WriteLine(ex.Message);
            }
            //Do after...
            System.Console.WriteLine("Do after...");
        }
    }
}
