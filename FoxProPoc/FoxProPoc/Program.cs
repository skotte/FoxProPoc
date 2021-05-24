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
            var dbFileName = "bmsw.dbc";
            Console.WriteLine("Enter FoxPro database folder location");
            var folderPath = Console.ReadLine();
            if(string.IsNullOrWhiteSpace(folderPath) || !Directory.Exists(folderPath))
            {
                //folderPath = "C:\\Users\\Sreenivas\\Documents\\Visual Foxpro 6.0\\Visual Foxpro 6.0";
                folderPath = "C:\\Users\\Sreenivas\\Documents\\Stretto\\TW_1133_Blank_Data\\data\\";
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
                var primaryKey = tbl.Attributes["PrimaryKey"].Value.Split(',');
                //var keys = primaryKey.Split(',');
                var selectSql = new OleDbCommand($"select * from {tblName}", connection);
                adapter.SelectCommand = selectSql;
                adapter.Fill(result);
                //var primeKeyCol = new Dictionary<string, object>();
                var primeKeyDetails = new Dictionary<string, Tuple<string,object>>();
                foreach (DataRow dataRow in result.Rows)
                {                     
                    var subSql = new StringBuilder();
                    foreach (System.Xml.XmlNode child in tbl.ChildNodes)
                    {
                        var column = child.Attributes["Name"].Value;
                        var algorithm = child.Attributes["ScramblingAlgorithm"].Value;
                        var columnDataType = result.Columns[column].DataType.Name;

                        //primaryKeyValue = dataRow[primaryKey].ToString();
                        var newValue = ScrambleColumnValue(dataRow[column].ToString(), algorithm);

                        if (!string.IsNullOrEmpty(subSql.ToString())) subSql.Append(",");
                        var newVal = columnDataType == "String" ? $"\"{newValue}\"" : newValue;

                        subSql.Append($" {tblName}.{column} = {newVal} "); 
                    }

                    foreach (var key in primaryKey)
                    {
                        var columnDataType = result.Columns[key].DataType.Name;
                        //primeKeyCol.Add(key, dataRow[key]);
                        primeKeyDetails.Add(key, new Tuple<string, object>(columnDataType, dataRow[key]));
                    }

                    connection.Open();
                    var sql = $"UPDATE {tblName} SET {subSql.ToString()} WHERE { BuildWhereClause(primeKeyDetails)}";
                    //var sql = $"UPDATE {tblName} SET {subSql.ToString()} WHERE { primaryKey} = {primaryKeyValue}";
                    var updateSql = new OleDbCommand(sql, connection);
                    adapter.UpdateCommand = updateSql;
                    adapter.UpdateCommand.ExecuteNonQuery();
                    connection.Close();
                }
            }           
        }

        

        private static string BuildWhereClause(Dictionary<string, Tuple<string, object>> primeKeyCol)
        {
            var sql = new StringBuilder();
            foreach (var primary in primeKeyCol)
            {
                if (!string.IsNullOrEmpty(sql.ToString())) sql.Append(" AND ");
                var newVal = primary.Value.Item1 == "String" ? $"\"{primary.Value.Item2}\"" : primary.Value.Item2;
                sql.Append($"{ primary.Key} = {newVal}");
            }

            return sql.ToString();
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

        private static void InsertData(string dbFilePath)
        {
            var adapter = new OleDbDataAdapter();
            var connection = new OleDbConnection($"Provider=VFPOLEDB.1;Data Source={dbFilePath};mode=share exclusive");
            connection.Open();

            var insertSql = new OleDbCommand("Insert into Accounts (case_no, account_no, bank_code, type, status, bankstatus, descr, ledger_bal, bankbk_bal, bankav_bal,banksh_bal, accr_int, opened, closed, last_recon, last_activ, total_dep, total_disb, next_check, next_dep,next_memo, next_adj, bnkbkbal01, bnkbkbal02, bnkbkbal03, bnkbkbal04, bnkbkbal05, bnkbkbal06, bnkbkbal07, bnkbkbal08,bnkbkbal09, bnkbkbal10, bnkbkbal11, bnkbkbal12, int_rate, term, principal, is_special, inst_code, mat_date,est_value, rollover, tickle, ent_by, ent_date, ent_time, approved, appr_by, sent_open, sent_otime,sent_close, sent_ctime, order, int_type, tda_appr, recon_done, reconcile_fl, rolldisp, rollconf, rollprinc,rollterm, rollby, rollsent, rollnewterm, rollnewprinc, backupwithholding, rollnewtransferaccount, lastbankmaturity, rate_code, dep_ledg_cat_key) values  (\"123456789012345\", \"7896542315\", \"1234\", \"DDA\", \"O\", \"O\", \"Chase Bank\",11,12,13,14,1234,{d'1968-04-28'},{d'1968-04-28'},{d'1968-04-28'},{d'1968-04-28'},1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,1000,15,16,17,1,\"1234\",{ d'1968-04-28'},1111,{ d'1968-04-28'},123,\"12345678\",{d'1968-04-28'},\"12345\",\"A\",\"12345678\",{d'1968-04-28'},\"12345\", {d'1968-04-28'},\"12345\",\"123\",\"C\",\"1234567890\",{d'1968-04-28'},1,{d'1968-04-28'},\"1\",123456,123,\"12345678\",{d'1968-04-28'},123,123,1,\"123456\",{d'1968-04-28'},\"12345\",\"12345\")", connection);

            adapter.InsertCommand = insertSql;
            adapter.InsertCommand.ExecuteNonQuery();
            connection.Close();
        }
    }
}
