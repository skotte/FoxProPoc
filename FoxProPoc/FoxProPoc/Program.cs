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
             
            try
            {
                LoadMappingData(fullPath);  
            }
            catch (Exception e)
            { 
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
    }
}
