using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Xml.Linq;

namespace ConsoleAppConnectDB
{
    class Program
    {
        static void Main(string[] args)
        {
            var dbFileName = "Company.dbc";
            Console.WriteLine("Enter FoxPro database folder location");
            var folderPath = Console.ReadLine();
            if(string.IsNullOrWhiteSpace(folderPath) || !Directory.Exists(folderPath))
            {
                folderPath = "C:\\Users\\Sreenivas\\Documents\\Visual Foxpro 6.0\\Visual Foxpro 6.0";
            }
            var fullPath = Path.Combine(folderPath, dbFileName);
            
            var connection = new OleDbConnection($"Provider=VFPOLEDB.1;Data Source={fullPath};mode=share exclusive");
                       
            var result = new DataTable();

            var random = new Random();
            const string chars = "123 Main St";
            var st = new string (Enumerable.Repeat(chars, chars.Length)
              .Select(s => s[random.Next(s.Length)]).ToArray());

            //connection.Open();
             
            try
            {

                LoadMappingData(fullPath);                

                //var adapter = new OleDbDataAdapter();
                 
                //var selectSql = new OleDbCommand("select * from customer", connection);
                //var updateSql = new OleDbCommand("UPDATE customer SET customer.FirstName = \"Manning\" WHERE customer.customerId = 2 ", connection);

                ////OleDbCommand deleteSqlExl = new OleDbCommand("USE saleorders EXCLUSIVE", connection);
                ////OleDbCommand deleteSqlQry = new OleDbCommand("DELETE FROM saleorders", connection); 
                ////OleDbCommand deleteSqlPack = new OleDbCommand("PACK", connection);

                ////deleteSqlExl.ExecuteNonQuery();
                ////deleteSqlQry.ExecuteNonQuery();
                ////deleteSqlPack.ExecuteNonQuery();
                
                //adapter.SelectCommand = selectSql;
                //adapter.Fill(result);
                
                
                //foreach (DataRow dataRow in result.Rows)
                //{
                //    foreach (var item in dataRow.ItemArray)
                //    {
                //        Console.Out.WriteLine(item);
                //    }
                //}

                //connection.Close();
                // }
            }
            catch (Exception e)
            {
                
                var ex = e;
                Console.Out.WriteLine(e.Message);
            }
            

            Console.ReadKey();
        }

        

        private static void LoadMappingData(string dbFilePath)
        {   
            /*             
            1.Load xml mapping file.
            2.Loop throgh tables
            3.Get data from VoxPro database by table name
            4.Loop through each data row
            5.Loop through columns from config file
            6.Apply scramble logic on each column
            9.Update row
             */

            /*
            1.Handle multiple primary keys
            2.Apply Trim => Check 
            3.Check the data types => Update config file and update updatesql
            
             */

            var filename = "ScramblerMapping.xml";             
            var filepath = Path.Combine(Environment.CurrentDirectory, "Metadata", filename);          
             

            //var doc1 = new System.Xml.XmlDocument();
            //doc1.Load(@"ScramblerMapping.xml");

            XElement xmlDoc = XElement.Load(filepath);
            var doc = new System.Xml.XmlDocument();

            doc.Load(filepath);
            var adapter = new OleDbDataAdapter();
            var tableNodes = doc.SelectNodes("ScrambleTable/Table");
            var connection = new OleDbConnection($"Provider=VFPOLEDB.1;Data Source={dbFilePath};mode=share exclusive");
           
            foreach (System.Xml.XmlNode tbl in tableNodes)
            {
                var result = new DataTable();
                var tblName = tbl.Attributes["Name"].Value;
                var primaryKey = tbl.Attributes["PrimaryKey"].Value;
                var selectSql = new OleDbCommand($"select * from {tblName}", connection);
                adapter.SelectCommand = selectSql;
                adapter.Fill(result);
                var primaryKeyValue = string.Empty;
                
                foreach (DataRow dataRow in result.Rows)
                {                     
                    var subSql = new StringBuilder();
                    foreach (System.Xml.XmlNode child in tbl.ChildNodes)
                    {
                        var column = child.Attributes["Name"].Value;
                        var algorithm = child.Attributes["ScramblingAlgorithm"].Value;
                        primaryKeyValue = dataRow[primaryKey].ToString();
                        var newValue = ScrambleColumnValue(dataRow[column].ToString(), algorithm);

                        if (!string.IsNullOrEmpty(subSql.ToString())) subSql.Append(",");
                        subSql.Append($" {tblName}.{column} =  \"{newValue}\" "); 
                    }

                    connection.Open();
                    var sql = $"UPDATE {tblName} SET {subSql.ToString()} WHERE { primaryKey} = {primaryKeyValue}";
                    var updateSql = new OleDbCommand(sql, connection);
                    adapter.UpdateCommand = updateSql;
                    adapter.UpdateCommand.ExecuteNonQuery();
                    connection.Close();
                }
            }           
        }        

        private static string ScrambleColumnValue(string columnValue, string algorithm)
        {
           
            switch (algorithm)
            {
                case "Account_No":
                    return AccountNoAlgorithm(columnValue);

                case "Standard":
                    return StandardAlgorithm(columnValue);

                default:
                    return columnValue;
            }
        }

        private static string StandardAlgorithm(string val)
        {
            if (string.IsNullOrWhiteSpace(val)) return val;

            char[] charArray = val.ToCharArray();
            Array.Reverse(charArray);
            return new string(charArray);
        }

        private static string AccountNoAlgorithm(string accountNo)
        {
            if (string.IsNullOrWhiteSpace(accountNo) || accountNo.Length < 10) return accountNo;
             
            accountNo = ReplaceAccountNumber(accountNo, 2, 1);
            accountNo = ReplaceAccountNumber(accountNo, 3, 2);
            accountNo = ReplaceAccountNumber(accountNo, 9, 3); 

            return accountNo;
        }

        private static string ReplaceAccountNumber(string accountNo, int index, int offSetValue)
        {
            var oldValue = accountNo.Substring(index, 1);
            int newValue = 0;
            if (int.TryParse(oldValue, out var i))
            {
                newValue = i + offSetValue;
                newValue = newValue >= 10 ? newValue - 10 : newValue;
            }
            char[] ch = accountNo.ToCharArray();
            ch[index] = Convert.ToChar(newValue.ToString());
             
            return new string(ch);
        }

        /*  static void Main(string[] args)
          {

              //// This will get the current WORKING directory (i.e. \bin\Debug)
              //string workingDirectory = Environment.CurrentDirectory;
              //// or: Directory.GetCurrentDirectory() gives the same result

              //// This will get the current PROJECT bin directory (ie ../bin/)
              //string projectDirectory = Directory.GetParent(workingDirectory).Parent.FullName;

              //// This will get the current PROJECT directory
              //string projectDirectory1 = Directory.GetParent(workingDirectory).Parent.Parent.FullName;

              OleDbConnection connection = new OleDbConnection("Provider=VFPOLEDB.1;Data Source=C:\\FoxProDatabase\\School.dbc;");
              OleDbConnection strettoConnection = new OleDbConnection("Provider=VFPOLEDB.1;Data Source=C:\\Users\\Sreenivas\\Downloads\\Visual Foxpro 6.0\\Visual Foxpro 6.0\\Stretto.dbc;mode=share exclusive");

              //C:\Users\Sreenivas\Downloads\Visual Foxpro 6.0\Visual Foxpro 6.0\\Stretto.dbc
              var result = new DataTable();

              //it may be possible to issue more than one command per open connection, it was unneccessary for me so i haven't tested it.
              strettoConnection.Open();

              try
              {
                  //if (connection.State == ConnectionState.Open)
                  //{
                  //var tableInfo = connection.GetSchema("Tables");

                  //DataTable tables = connection.GetSchema(
                  //    System.Data.OleDb.OleDbMetaDataCollectionNames.Tables
                  //);

                  //foreach (System.Data.DataRow rowTables in tables.Rows)
                  //{
                  //    Console.Out.WriteLine(rowTables["table_name"].ToString());
                  //    DataTable columns = connection.GetSchema(
                  //        System.Data.OleDb.OleDbMetaDataCollectionNames.Columns,
                  //        new String[] { null, null, rowTables["table_name"].ToString(), null }
                  //    );
                  //    foreach (System.Data.DataRow rowColumns in columns.Rows)
                  //    {
                  //        Console.Out.WriteLine(
                  //            rowTables["table_name"].ToString() + "." +
                  //            rowColumns["column_name"].ToString() + " = " +
                  //            rowColumns["data_type"].ToString()
                  //        );
                  //    }
                  //}


                  var adapter = new OleDbDataAdapter();

                  var selectSql = new OleDbCommand("select * from library", connection);
                  var selectSqlSt = new OleDbCommand("select * from customer", strettoConnection);
                  var updateSql = new OleDbCommand("UPDATE customer SET customer.name = \"Manning\" WHERE customer.Id = 2 ", strettoConnection);


                  //var deleteCmd = @"SET EXCLUSIVE ON
                  //            DELETE FROM library WHERE Id = 1
                  //            PACK";

                  //var deleteCmd = @"use library Exclusive DELETE FROM library WHERE Id = 2 pack";



                  //connection.Open();
                  //deleteSql.ExecuteNonQuery();
                  //deleteSql1.ExecuteNonQuery();
                  //deleteSql2.ExecuteNonQuery();
                  //connection.Open();

                  //OleDbCommand cmd = new OleDbCommand();
                  //cmd.Connection = strettoConnection;
                  //cmd.CommandType = CommandType.Text;
                  //cmd.CommandText = "USE customer EXCLUSIVE";
                  //OleDbCommand cmd1 = new OleDbCommand();
                  //cmd1.Connection = strettoConnection;
                  //cmd1.CommandType = CommandType.Text;
                  //cmd1.CommandText = "DELETE FROM customer";
                  //OleDbCommand cmd2 = new OleDbCommand();
                  //cmd2.Connection = strettoConnection;
                  //cmd2.CommandType = CommandType.Text;
                  //cmd2.CommandText = "PACK";

                  OleDbCommand cmd = new OleDbCommand("USE customer EXCLUSIVE", strettoConnection);
                  OleDbCommand cmd1 = new OleDbCommand("DELETE FROM customer", strettoConnection);

                  OleDbCommand cmd3 = new OleDbCommand("USE Sales EXCLUSIVE", strettoConnection);
                  OleDbCommand cmd4 = new OleDbCommand("DELETE FROM Sales", strettoConnection);

                  OleDbCommand cmd2 = new OleDbCommand("PACK", strettoConnection);


                  //connection.Open();

                  cmd3.ExecuteNonQuery();
                  cmd4.ExecuteNonQuery();
                  cmd2.ExecuteNonQuery();
                  cmd.ExecuteNonQuery();
                  cmd1.ExecuteNonQuery();
                  cmd2.ExecuteNonQuery();
                  //connection.Close();


                  //deleteSql.ExecuteNonQuery();

                  //DataSet catDS = new DataSet();
                  //adapter.UpdateCommand = updateSql;
                  //adapter.UpdateCommand.ExecuteNonQuery();

                  //adapter.DeleteCommand = connection.CreateCommand();
                  //adapter.DeleteCommand.CommandText = sb.ToString();
                  //adapter.DeleteCommand.ExecuteNonQuery();

                  //cmd.CommandType = CommandType.Text;
                  //cmd.CommandText = "USE mytable EXCLUSIVE";
                  //adapter.SelectCommand = selectSqlSt;
                  //adapter.Fill(result);
                  //adapter.Update(result);
                  ////DataRow cRow = catDS.Tables["class"].Rows[0];
                  ////cRow["classname"] = "FirstGradeUpdate";
                  //adapter.Update(catDS);
                  //foreach (DataRow dataRow in result.Rows)
                  //{
                  //    foreach (var item in dataRow.ItemArray)
                  //    {
                  //        Console.Out.WriteLine(item);
                  //    }
                  //}

                  connection.Close();
                  // }
              }
              catch (Exception e)
              {

                  var ex = e;
              }


              Console.ReadKey();
          }*/
    }
}
