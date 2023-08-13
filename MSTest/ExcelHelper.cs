using CommonLibrary;
using DTO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Threading;

namespace MSTest
{
    [TestClass]
    public class ExcelHelper
    {
        [TestMethod]
        public void ExcelTest()
        {
            CommonExcel.MiniExcelExport();
        }

        [TestMethod]
        public void DataInsert()
        {
            //var dataList = CommonHelper.GetTestListDto(5000);
            //var dataListTwo = CommonHelper.GetTestListDtoTwo(5000);
            //var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            //var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            //SqlBulkCopyHelper.BulkInsertTables(new List<DataTable>() { table, tableTwo }).Wait();
            //sw.Stop();
            //string bb = sw.Elapsed.TotalSeconds.ToString();
            //Console.WriteLine(bb);

            //var aa = SqlHelper.GetField<int>("select PwId from Labor_ProjectWorker");
            //var aaa = SqlHelper.GetField<int>("select count(1) from Labor_ProjectWorker");
            //var aaaa = SqlHelper.GetField<Guid>("select ProId from Labor_ProjectWorker");
            //var bb = SqlHelper.Get<dynamic>("select top 100 * from Labor_ProjectWorker");
            //var bbb = SqlHelper.Get<TestDto>("select top 100 * from Labor_ProjectWorker");
            //var bbbb = SqlHelper.Get<dynamic>("select top 100 PwId from Labor_ProjectWorker");
            //var cc = SqlHelper.GetList<dynamic>("select top 100 * from Labor_ProjectWorker");
            //var ccc = SqlHelper.GetList<TestDto>("select top 100 * from Labor_ProjectWorker");

            //var anonymous = new { PwId = 0, ProId = Guid.Empty };
            //var dd = SqlHelper.GetAnonymousList(anonymous.GetType(), "select top 100 * from Labor_ProjectWorker");

            //var ee = SqlHelper.GetPager<dynamic>("Labor_ProjectWorker", "ProId,PwId,WorkerName", "AddTime desc", 10, 1, default, out int totalCount);
            //var eee = SqlHelper.GetPager<TestDto>("Labor_ProjectWorker", "ProId,PwId,WorkerName", "AddTime desc", 10, 1, default, out int totalCount1);
            //var eeee = SqlHelper.GetAnonymousPager(anonymous.GetType(), "Labor_ProjectWorker", "ProId,PwId,WorkerName", "AddTime desc", 10, 1, default, out int totalCount2);
            //var ff = SqlHelper.GetById<dynamic>(35, "[Labor_SyncBlackList]","Id");


            //var aa = new { PwId = 1, WorkerName = "test" };
            //var aaa = JsonConvert.DeserializeObject<object>(JsonConvert.SerializeObject(aa));
            //dynamic aaaa = new System.Dynamic.ExpandoObject();
            //aaaa.PwId = 123; aaaa.WorkerName = "test";
            //var bbbb = new TestDto(); bbbb.PwId = 10; bbbb.WorkerName = "test";

            //var tt = SqlHelper.Add<TestDto>(aaaa);

            //aaaa.PwId = 888;
            //var ttt = SqlHelper.Set<TestDto>(aaaa, new { Id = 18 });


            //dynamic dyc = new System.Dynamic.ExpandoObject();
            //dyc.SyncType = 15;
            //dyc.ProId = Guid.NewGuid();
            //dyc.CreateUser = "CreateUser";
            //dyc.CreateTime = DateTime.Now;

            //var dyc = new
            //{
            //    SyncType = 15,
            //    ProId = Guid.NewGuid(),
            //    CreateUser = "CreateUser",
            //    CreateTime = DateTime.Now
            //};

            //int ttt = SqlHelper.Add<Sync_DataTransfer>(dyc);

            //var aa = new { PwId = 0, ProId = Guid.Empty, WorkerId = default(int?) };


            //var aaList = SqlHelper.ObjectToTable(new List<object>() { aa, aa });

            //var aaa = JsonConvert.DeserializeObject<List<dynamic>>(JsonConvert.SerializeObject(new List<object>() { aa, aa }));

            //dynamic eee = new System.Dynamic.ExpandoObject();
            //eee.aa = 1;
            //eee.bb = Guid.Empty;

            //var aaaList = SqlHelper.ObjectToTable(aaa);


            //var tttt = SqlHelper.GetPager<dynamic>("dbo.Labor_ProjectWorker pw INNER JOIN dbo.Labor_Worker w ON pw .WorkerId=w.WorkerId", "pw.PwId,w.WorkerId", "w.WorkerId", 20, 1, "", out int totalCount1);

            //var bb = SqlHelper.GetAnonymousPager(new { PwId = 0, WorkerId = 0 }.GetType(), "dbo.Labor_ProjectWorker pw INNER JOIN dbo.Labor_Worker w ON pw .WorkerId=w.WorkerId", "pw.PwId,w.WorkerId", "w.WorkerId", 20, 1, "", out int totalCount2);
            //var eeee = SqlHelper.GetAnonymousPager(new { PwId = 0, ProId = Guid.Empty }.GetType(), "Labor_ProjectWorker", "ProId,PwId,WorkerName", "AddTime desc", 10, 1, default, out int totalCount3);
            //dyc.UpdateUser = "UpdateUser";
            //int bbb = SqlHelper.Set<Sync_DataTransfer>(dyc);

            //var bbbb = SqlHelper.GetList<Labor_ProjectWorker>("select top 10 * from Labor_ProjectWorker");
            //bbbb.ForEach(o => o.LastTrainingDate = DateTime.Now);
            var st = Stopwatch.StartNew();
            //var aa = SqlHelper.AddList<Labor_ProjectWorker>(bbbb);
            //var aa = SqlHelper.BulkAdd<Labor_ProjectWorker>(bbbb.Select(o => (object)o).ToList());
            //var aaa = SqlHelper.BulkSet<Labor_ProjectWorker>(bbbb.Select(o => (object)o).ToList());

            var aa = SqlHelper.GetList<Labor_ProjectWorker>("select top 1 * from Labor_ProjectWorker");
            aa.ForEach(o => o.LastTrainingDate = DateTime.Now);
            //var bb = SqlHelper.BulkCopyAddTran("Labor_ProjectWorker", aa.Select(o => (object)o).ToList());
            //var cc = SqlHelper.BulkCopySetTran("Labor_ProjectWorker", aa.Select(o => (object)o).ToList());

            var dd = SqlHelper.BulkCopyAddAndSet(new Dictionary<string, List<object>> { { "Labor_ProjectWorker", aa.Select(o => (object)o).ToList() } },
                new Dictionary<string, List<object>> { { "Labor_ProjectWorker", aa.Select(o => (object)o).ToList() } });

            st.Stop();
            var stt = st.Elapsed.TotalSeconds;

        }
        [TestMethod]
        public void DataUpdate()
        {
            var table = SqlCoreHelper.ExecuteDataSetText("select Id,FieldD from BulkTest", null).Tables[0];
            var tableTwo = SqlCoreHelper.ExecuteDataSetText("select Id,FieldD from BulkTestTwo", null).Tables[0];
            foreach (DataRow item in table.Rows)
            {
                item["FieldD"] = "111";
            }
            foreach (DataRow item in tableTwo.Rows)
            {
                item["FieldD"] = "222";
            }
            table.TableName = "BulkTest";
            tableTwo.TableName = "BulkTestTwo";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlBulkCopyHelper.BulkUpdateTables(new List<DataTable>() { table, tableTwo });
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
            Console.WriteLine(bb);
        }
        [TestMethod]
        public void DataEdit()
        {
            var dataList = CommonHelper.GetTestListDto(5000);
            var dataListTwo = CommonHelper.GetTestListDtoTwo(5000);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");

            var tableUpdate = SqlCoreHelper.ExecuteDataSetText("select top 5000 Id,FieldD from BulkTest order by FieldU asc", null).Tables[0];
            var tableTwoUpdate = SqlCoreHelper.ExecuteDataSetText("select top 5000 Id,FieldD from BulkTestTwo order by FieldU asc", null).Tables[0];
            foreach (DataRow item in tableUpdate.Rows)
            {
                item["FieldD"] = "555";
            }
            foreach (DataRow item in tableTwoUpdate.Rows)
            {
                item["FieldD"] = "666";
            }
            tableUpdate.TableName = "BulkTest";
            tableTwoUpdate.TableName = "BulkTestTwo";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlBulkCopyHelper.BulkEditTables(new List<DataTable>() { table, tableTwo }, new List<DataTable>() { tableUpdate, tableTwoUpdate });
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
            Console.WriteLine(bb);
        }
        [TestMethod]
        public void Test()
        {
            var str = 0 + null;
            var str1 = "" + null;
            var dataList = CommonHelper.GetTestListDto(10);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var bb = new { Id = Guid.Empty, FieldB = string.Empty, FieldU = default(DateTime?), FieldV = default(decimal?), FieldX = default(int) };
            var dd = table.ToAnonymousList(bb);
            var test = new object().GetType();
            var test1 = new System.Dynamic.ExpandoObject().GetType();
            var test2 = bb.GetType();
        }
    }
}
