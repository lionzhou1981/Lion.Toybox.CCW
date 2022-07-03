using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MySql.Data.MySqlClient;
using Lion;
using Lion.Encrypt;
using Lion.Net;
using Lion.Data;
using Lion.Data.MySqlClient;
using Lion.CryptoCurrency.Ethereum;
using CCW.Core;

namespace CCW.Service
{
    internal class NodeETH
    {
        private static string Chain = "ETH";
        private static long ChainId = 0;
        private static string Secret = "";
        private static string Host = "";
        private static int Confirm = 999;
        private static string MergeAddress = "";
        private static Thread ThreadAddress;
        private static Thread ThreadList;
        private static Thread ThreadCheck;
        private static Thread ThreadSend;

        public static void Init()
        {
            ChainId = Common.Settings["NodeETH"]["ChainId"].Value<long>();
            Secret = Common.Settings["NodeETH"]["Secret"].Value<string>();
            Host = Common.Settings["NodeETH"]["Host"].Value<string>();
            Confirm = Common.Settings["NodeETH"]["Confirm"].Value<int>();
            MergeAddress = Common.Settings["NodeETH"]["MergeAddress"].Value<string>();

            ThreadAddress = new Thread(new ThreadStart(GenerateAddress));
            ThreadAddress.Start();

            ThreadList = new Thread(new ThreadStart(ListTransactions));
            ThreadList.Start();

            ThreadCheck = new Thread(new ThreadStart(CheckTransactions));
            ThreadCheck.Start();

            ThreadSend = new Thread(new ThreadStart(SendTransactions));
            ThreadSend.Start();
        }

        #region GenerateAddress
        private static void GenerateAddress()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_address");
                        _tsql.Fields.Add("COUNT(*)");
                        _tsql.Wheres.And("chain", "=", Chain);
                        _tsql.Wheres.And("status", "=", 0);

                        long _count = (long)_db.GetDataScalar(_tsql.ToSqlCommand());
                        if (_count > 50) { continue; }

                        for (int i = 0; i < 100; i++)
                        {
                            Address _address = Address.Generate();

                            _tsql = new TSQL(TSQLType.Insert, "wallet_address");
                            _tsql.Fields.Add("chain", "", Chain);
                            _tsql.Fields.Add("address", "", _address.Text.ToLower());
                            _tsql.Fields.Add("source", "", OpenSSLAes.Encode(_address.Private, Secret));
                            _tsql.Fields.Add("create_at", "", _now);
                            _tsql.Fields.Add("status", "", 0);
                            _db.Execute(_tsql.ToSqlCommand());

                            Common.Log("GenerateAddress", $"{_address.Text}");
                        }
                    }
                }
                catch (Exception _ex)
                {
                    Common.Log("GenerateAddress", _ex.ToString(), LogLevel.ERROR);
                }
            }
        }
        #endregion

        #region ListTransactions
        private static void ListTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                BigInteger _local = -1;
                BigInteger _block = -1;
                BigInteger _height = -1;

                #region Step 0: 获取补扫区块
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_block");
                        _tsql.Limit = 1;
                        _tsql.Wheres.And("chain", "=", Chain);
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Orders.AscField("id");
                        DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                        if (_table.Rows.Count == 1)
                        {
                            _block = BigInteger.Parse(_table.Rows[0]["block"].ToString());
                            Common.Log("ListTransactions", $"Step 0: {_block}");
                        }
                    }
                }
                catch (Exception _ex)
                {
                    _block = -1;
                    Common.Log("ListTransactions", $"Step 0: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 1: 获取本地区块
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_chain");
                        _tsql.Fields.Add("block");
                        _tsql.Wheres.And("code", "=", Chain);
                        _local = BigInteger.Parse(_db.GetDataScalar(_tsql.ToSqlCommand()).ToString());
                        Common.Log("ListTransactions", $"Step 1: {_local}");
                    }
                }
                catch (Exception _ex)
                {
                    _local = -1;
                    Common.Log("ListTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 2: 获取节点区块
                if (_block == -1)
                {
                    try
                    {
                        JObject _result = Call("eth_blockNumber");
                        if (_result == null) { throw new Exception("eth_blockNumber"); }

                        _height =  BigNumberPlus.HexToBigInt(_result["result"].ToString()) - BigInteger.One;
                        Common.Log("ListTransactions", $"Step 2: {_height}");
                    }
                    catch (Exception _ex)
                    {
                        _height = -1;
                        Common.Log("ListTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                    }

                    if (_local == -1 || _height == -1) { Common.Log("ListTransactions", $"Block count failed. {_local}/{_height}", LogLevel.ERROR); continue; }
                    if (_local >= _height) { continue; }

                    _block = _local + 1;
                }
                #endregion

                #region Step 3: 获取区块数据
                string _hash = "";
                DateTime _blockAt = DateTime.Parse("1900-1-1 0:0:0.0");
                JArray _txs = new JArray();
                try
                {
                    JObject _result = Call("eth_getBlockByNumber", "0x" + _block.ToString("X").TrimStart('0'), true);
                    if (_result == null || !_result.ContainsKey("result") || !_result["result"].Value<JObject>().ContainsKey("transactions")) { throw new Exception($"Get block failed. {_result}"); }

                    _hash = _result["result"]["hash"].Value<string>().ToLower();
                    _blockAt = DateTimePlus.JSTime2DateTime(HexPlus.HexToInt64(_result["result"]["timestamp"].Value<string>()));

                    JArray _arrays = new JArray();
                    if (_result["result"]["transactions"].Type == JTokenType.Array)
                    {
                        _arrays = _result["result"]["transactions"].Value<JArray>();
                    }
                    else
                    {
                        _arrays.Add(_result["result"]["transactions"].Value<JObject>());
                    }

                    for (int i = 0; i < _arrays.Count; i++)
                    {
                        JToken _tx = _arrays[i];
                        string _txid = _tx["hash"].Value<string>();
                        if (_tx["value"].ToString() == "0x0") { continue; }

                        string _value = Ethereum.HexToDecimal(_tx["value"].ToString());

                        BigInteger _blockNumber = BigNumberPlus.HexToBigInt(_tx["blockNumber"].Value<string>());
                        string _address = _tx["to"].Value<string>();

                        JObject _child = new JObject();
                        _child["txid"] = _txid;
                        _child["address"] = _address;
                        _child["amount"] = _value;
                        _txs.Add(_child);
                    }

                    Common.Log("ListTransactions", $"Step 3: {_txs.Count}");
                }
                catch (Exception _ex)
                {
                    Common.Log("ListTransactions", $"Step 3: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 4: 保存交易详情
                try
                {
                    int _null = 0, _find = 0, _save = 0;
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql;
                        foreach (JObject _tx in _txs)
                        {
                            #region 保存交易
                            string _txid = _tx["txid"].Value<string>().ToLower();
                            string _address = _tx["address"].Value<string>().ToLower();
                            decimal _amount = decimal.Parse(_tx["amount"].Value<string>());

                            _tsql = new TSQL(TSQLType.Select, "wallet_address");
                            _tsql.Fields.Add("member_id");
                            _tsql.Wheres.And("address", "=", _address);
                            _tsql.Wheres.And("chain", "=", Chain);
                            DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                            if (_table.Rows.Count == 0) { continue; }

                            _find++;
                            string _member_id = _table.Rows[0]["member_id"].ToString();

                            _tsql = new TSQL(TSQLType.Select, "wallet_transaction");
                            _tsql.Fields.Add("COUNT(*)");
                            _tsql.Wheres.And("txid", "=", _txid);
                            _tsql.Wheres.And("txindex", "=", 0);
                            _tsql.Wheres.And("chain", "=", Chain);
                            long _count = (long)_db.GetDataScalar(_tsql.ToSqlCommand());
                            if (_count != 0) { Common.Log("ListTransactions", $"Step 5: {Chain} {_txid} exist", LogLevel.WARN); continue; };

                            _tsql = new TSQL(TSQLType.Insert, "wallet_transaction");
                            _tsql.Fields.Add("chain", "", Chain);
                            _tsql.Fields.Add("token", "", "");
                            _tsql.Fields.Add("txid", "", _txid);
                            _tsql.Fields.Add("txindex", "", 0);
                            _tsql.Fields.Add("tx_at", "", _blockAt);
                            _tsql.Fields.Add("block", "", _block);
                            _tsql.Fields.Add("address", "", _address);
                            _tsql.Fields.Add("member_id", "", _member_id);
                            _tsql.Fields.Add("amount", "", _amount);
                            _tsql.Fields.Add("status", "", 0);
                            _tsql.Fields.Add("list_at", "", DateTime.UtcNow);
                            _tsql.Fields.Add("deposit", "", 0);
                            _db.Execute(_tsql.ToSqlCommand());
                            _save++;
                            Common.Log("ListTransactions", $"Step 4: {Chain} {_txid} {_txid} {_address} {_amount}");
                            #endregion
                        }

                        #region 保存区块
                        _tsql = new TSQL(TSQLType.Select, "wallet_block");
                        _tsql.Wheres.And("block", "=", _block);
                        _tsql.Wheres.And("chain", "=", Chain);
                        DataTable _tableBlock = _db.GetDataTable(_tsql.ToSqlCommand());
                        if (_tableBlock.Rows.Count == 0)
                        {
                            _tsql = new TSQL(TSQLType.Insert, "wallet_block");
                            _tsql.Fields.Add("chain", "", Chain);
                            _tsql.Fields.Add("block", "", _block);
                            _tsql.Fields.Add("block_at", "", _blockAt);
                            _tsql.Fields.Add("hash", "", _hash.ToLower());
                            _tsql.Fields.Add("tx", "", _txs.Count);
                            _tsql.Fields.Add("null", "", _null);
                            _tsql.Fields.Add("find", "", _find);
                            _tsql.Fields.Add("save", "", _save);
                            _tsql.Fields.Add("status", "", 1);
                            _db.Execute(_tsql.ToSqlCommand());
                        }
                        else
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_block");
                            _tsql.Fields.Add("tx", "", _txs.Count);
                            _tsql.Fields.Add("null", "", _null);
                            _tsql.Fields.Add("find", "", _find);
                            _tsql.Fields.Add("save", "", _save);
                            _tsql.Fields.Add("status", "", 1);
                            _tsql.Wheres.And("id", "=", _tableBlock.Rows[0]["id"]);
                            _db.Execute(_tsql.ToSqlCommand());
                        }
                        #endregion

                        #region 保存高度
                        if (_height != -1)
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_chain");
                            _tsql.Fields.Add("block", "", _block);
                            _tsql.Wheres.And("code", "=", Chain);
                            _db.Execute(_tsql.ToSqlCommand());
                        }
                        #endregion
                    }
                }
                catch (Exception _ex)
                {
                    Common.Log("ListTransactions", $"Step 4: {_ex}", LogLevel.ERROR);
                    return;
                }
                #endregion

                if (_height == -1 || _height > _block) { _last = _last.AddDays(-1); }
            }
        }
        #endregion

        #region CheckTransactions
        private static void CheckTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                #region Step 1: 获取入账交易
                DataTable _transactionList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_transaction");
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Wheres.And("chain", "=", Chain);
                        _transactionList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("CheckTransactions", $"Step 1: {_transactionList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _transactionList = null;
                    Common.Log("CheckTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 2: 检查入账交易
                if (_transactionList != null)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            foreach (DataRow _row in _transactionList.Rows)
                            {
                                string _txid = _row["txid"].ToString();
                                JObject _tx = GetTransaction(_txid, out BigInteger _blockNumber);
                                if (_tx == null) { continue; }

                                int _confirm = 0;
                                if (_tx["address"].Value<string>() != _row["address"].ToString()) { _confirm = -1; break; }
                                if (_tx["amount"].Value<decimal>() != (decimal)_row["amount"]) { _confirm = -1; break; }
                                if (_tx["balance"].Value<decimal>() < (decimal)_row["amount"]) { _confirm = -1; break; }
                                _confirm = _tx["confirmations"].Value<int>();

                                if (_confirm == (int)_row["confirm"]) { continue; }

                                TSQL _tsql = new TSQL(TSQLType.Update, "wallet_transaction");
                                if (_confirm > Confirm)
                                {
                                    _tsql.Fields.Add("status", "", 1);
                                }
                                else if (_confirm == -1)
                                {
                                    _tsql.Fields.Add("status", "", -1);
                                }
                                _tsql.Fields.Add("confirm", "", _confirm);
                                _tsql.Fields.Add("deposit", "", 0);
                                _tsql.Wheres.And("id", "=", _row["id"]);
                                _db.Execute(_tsql.ToSqlCommand());
                                Common.Log("CheckTransactions", $"Step 2: {_txid} {_confirm}");
                            }
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("CheckTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                    }
                }
                #endregion

                #region Step 3: 获取出账交易
                DataTable _withdrawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_withdraw");
                        _tsql.Wheres.And("status", "=", 2);
                        _tsql.Wheres.And("tx_at", "<", DateTime.UtcNow.AddHours(-1));
                        _tsql.Wheres.And("chain", "=", Chain);
                        _withdrawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("CheckTransactions", $"Step 3: {_withdrawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _withdrawList = null;
                    Common.Log("CheckTransactions", $"Step 3: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 4: 检查出账交易
                if (_withdrawList != null)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            foreach (DataRow _row in _withdrawList.Rows)
                            {
                                string _txid = _row["txid"].ToString();
                                JObject _tx = GetTransaction(_txid,out BigInteger _blockNumber);

                                int _confirm = _tx["confirmations"].Value<int>();
                                if (_confirm <= (int)_row["confirm"]) { continue; }

                                TSQL _tsql = new TSQL(TSQLType.Update, "wallet_block");
                                _tsql.Wheres.And("block", "=", _blockNumber.ToString());
                                _tsql.Wheres.And("chain", "=", Chain);
                                DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                                if (_table.Rows.Count == 0) { continue; }

                                DateTime _blocktime = (DateTime)_table.Rows[0]["block_at"];

                                _tsql = new TSQL(TSQLType.Update, "wallet_withdraw");
                                _tsql.Fields.Add("block", "", _blockNumber.ToString());
                                _tsql.Fields.Add("block_at", "", _blocktime);
                                _tsql.Fields.Add("confirm", "", _confirm);
                                _tsql.Fields.Add("confirm_at", "", DateTime.UtcNow);
                                if (_confirm > Confirm) { _tsql.Fields.Add("status", "", 5); }
                                _tsql.Fields.Add("withdraw", "", 0);
                                _tsql.Wheres.And("id", "=", _row["id"]);
                                _db.Execute(_tsql.ToSqlCommand());
                                Common.Log("CheckTransactions", $"Step 4: {_txid} {_confirm}");
                            }
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("CheckTransactions", $"Step 4: {_ex}", LogLevel.ERROR);
                    }
                }
                #endregion

                #region Step 5: 获取RAW 交易
                DataTable _rawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_sendraw");
                        _tsql.Wheres.And("status", "=", 2);
                        _tsql.Wheres.And("tx_at", "<", DateTime.UtcNow.AddMinutes(-10));
                        _tsql.Wheres.And("chain", "=", Chain);
                        _rawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("CheckTransactions", $"Step 5: {_rawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _withdrawList = null;
                    Common.Log("CheckTransactions", $"Step 5: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 6: 检查RAW 交易
                if (_rawList != null)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            foreach (DataRow _row in _rawList.Rows)
                            {
                                string _txid = _row["txid"].ToString();
                                JObject _tx = GetTransaction(_txid, out BigInteger _blockNumber);

                                int _confirm = _tx["confirmations"].Value<int>();
                                if (_confirm <= (int)_row["confirm"]) { continue; }

                                TSQL _tsql = new TSQL(TSQLType.Update, "wallet_block");
                                _tsql.Wheres.And("block", "=", _blockNumber.ToString());
                                _tsql.Wheres.And("chain", "=", Chain);
                                DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                                if (_table.Rows.Count == 0) { continue; }

                                DateTime _blocktime = (DateTime)_table.Rows[0]["block_at"];

                                _tsql = new TSQL(TSQLType.Update, "wallet_sendraw");
                                _tsql.Fields.Add("block", "", _blockNumber.ToString());
                                _tsql.Fields.Add("block_at", "", _blocktime);
                                _tsql.Fields.Add("confirm", "", _confirm);
                                _tsql.Fields.Add("confirm_at", "", DateTime.UtcNow);
                                if (_confirm > Confirm) { _tsql.Fields.Add("status", "", 5); }
                                _tsql.Wheres.And("id", "=", _row["id"]);
                                _db.Execute(_tsql.ToSqlCommand());
                                Common.Log("CheckTransactions", $"Step 6: {_txid} {_confirm}");
                            }
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("CheckTransactions", $"Step 6: {_ex}", LogLevel.ERROR);
                    }
                }
                #endregion
            }
        }
        #endregion

        #region SendTransactions
        private static void SendTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalSeconds < 30) { Thread.Sleep(100); continue; }
                _last = _now;

                #region Step 1: 获取出账交易
                TSQL _tsql;
                DataTable _withdrawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        _tsql = new TSQL(TSQLType.Select, "wallet_withdraw");
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Wheres.And("chain", "=", Chain);
                        _withdrawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("SendTransactions", $"Step 1: {_withdrawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _withdrawList = null;
                    Common.Log("SendTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                }
                if (_withdrawList == null) { continue; }
                #endregion

                #region Step 2: 出账交易锁定
                using (DbCommon _db = Common.DbCommonMain)
                {
                    try
                    {
                        _db.Open();
                        _db.BeginTransaction();
                        foreach (DataRow _row in _withdrawList.Rows)
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_withdraw");
                            _tsql.Fields.Add("status", "", 1);
                            _tsql.Fields.Add("withdraw", "", 0);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 0);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update withdraw failed. ({_row["id"]})"); }
                        }
                        _db.CommitTransaction();
                        Common.Log("SendTransactions", $"Step 2: {_withdrawList.Rows.Count}");
                    }
                    catch (Exception _ex)
                    {
                        _db.RollbackTransaction();
                        Common.Log("SendTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                        continue;
                    }
                }
                #endregion

                #region Step 3: 逐个交易出账
                _withdrawList.Columns.Add("send_status", typeof(int));
                _withdrawList.Columns.Add("send_result", typeof(string));
                foreach (DataRow _row in _withdrawList.Rows)
                {
                    try
                    {
                        _row["send_status"] = "1";
                        _row["send_result"] = "txid";
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("SendTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                        _row["send_status"] = "-1";
                        _row["send_result"] = _ex.Message;
                    }
                }
                Common.Log("SendTransactions", $"Step 2: {_withdrawList.Rows.Count}");
                #endregion

                #region Step 4: 出账完成更新
                int _failed = 0;
                using (DbCommon _db = Common.DbCommonMain)
                {
                    _db.Open();
                    foreach (DataRow _row in _withdrawList.Rows)
                    {
                        try
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_withdraw");
                            if (_row["send_status"].ToString() == "1") { _tsql.Fields.Add("status", "", 2); }
                            if (_row["send_status"].ToString() == "-1") { _tsql.Fields.Add("status", "", -9); _failed++; }
                            _tsql.Fields.Add("txid", "", _row["send_result"].ToString());
                            _tsql.Fields.Add("tx_at", "", DateTime.UtcNow);
                            _tsql.Fields.Add("withdraw", "", 0);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 1);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update withdraw failed. ({_row["id"]})"); }
                        }
                        catch (Exception _ex)
                        {
                            Common.Log("SendTransactions", $"Step 4: {_row["id"]} {_ex}", LogLevel.ERROR);
                        }
                    }
                    Common.Log("SendTransactions", $"Step 4: {_withdrawList.Rows.Count}/{_failed}");
                }
                #endregion
            }
        }
        #endregion

        #region SendRawTransactions
        private static void SendRawTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                #region Step 1: 获取出账交易
                TSQL _tsql;
                DataTable _rawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        _tsql = new TSQL(TSQLType.Select, "wallet_sendraw");
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Wheres.And("chain", "=", Chain);
                        _rawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("SendRawTransactions", $"Step 1: {_rawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _rawList = null;
                    Common.Log("SendRawTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                }
                if (_rawList == null || _rawList.Rows.Count == 0) { continue; }
                #endregion

                #region Step 2: 出账交易发出
                foreach (DataRow _row in _rawList.Rows)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            _tsql = new TSQL(TSQLType.Update, "wallet_sendraw");
                            _tsql.Fields.Add("status", "", 1);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 0);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update sendraw failed. ({_row["id"]})"); }

                            JObject _result = new JObject {
                                ["jsonrpc"] = "1.0",
                                ["id"] = "1",
                                ["method"] = "sendrawtransaction",
                                ["params"] = new JArray(_row["raw"].ToString())
                            };
                            //_result = Call(_result);
                            if (_result["error"].Type != JTokenType.Null) { throw new Exception(_result["error"]["message"].Value<string>()); }

                            string _txid = _result["result"].Value<string>();

                            _tsql = new TSQL(TSQLType.Update, "wallet_sendraw");
                            _tsql.Fields.Add("status", "", 2);
                            _tsql.Fields.Add("txid", "", _txid);
                            _tsql.Fields.Add("tx_at", "", DateTime.UtcNow);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 1);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update sendraw failed. ({_row["id"]})"); }

                            Common.Log("SendRawTransactions", $"Step 2: {_row["id"]} {_txid}");
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("SendRawTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                        continue;
                    }
                }
                #endregion
            }
        }
        #endregion

        #region GetTransaction
        private static JObject GetTransaction(string _txid,out BigInteger _blockNumber)
        {
            JObject _block, _tx, _receipt, _balance;
            _blockNumber = 0;
            try
            {
                _block = Call("eth_blockNumber", "1");
                if (_block == null) { throw new Exception("Call eth_blockNumber"); }

                _tx = Call("eth_getTransactionByHash", "1", _txid);
                if (_tx == null) { throw new Exception("Call eth_getTransactionByHash"); }
                _tx = _tx["result"].Value<JObject>();

                _receipt = Call("eth_getTransactionReceipt", "1", _txid);
                if (_receipt == null) { throw new Exception("Call eth_getTransactionReceipt"); }
                _receipt = _receipt["result"].Value<JObject>();

                string _status = _receipt.ContainsKey("status") ? _receipt["status"].Value<string>() : "";
                string _address = _tx["to"].Value<string>();

                BigInteger _currentNumber = BigNumberPlus.HexToBigInt(_block["result"].Value<string>()) - BigInteger.One;
                _blockNumber = BigNumberPlus.HexToBigInt(_tx["blockNumber"].Value<string>());
                BigInteger _confirmNumber = _currentNumber - _blockNumber;

                _balance = Call("eth_getBalance", "1", _address, "latest");
                if (_balance == null) { throw new Exception($"Call eth_getBalance {_address}"); }
                _balance = _balance["result"].Value<JObject>();

                string _balanceNumber = Ethereum.HexToDecimal(_balance.Value<string>());
                string _amountNumber = Ethereum.HexToDecimal(_tx["value"].ToString());

                JObject _result = new JObject();
                _result["txid"] = _txid;
                _result["address"] = _address;
                _result["amount"] = _amountNumber;
                _result["confirmations"] = _status != "0x1" ? "-9" : _confirmNumber.ToString();
                _result["balance"] = _balanceNumber;
                return _result;
            }
            catch (Exception _ex)
            {

                Common.Log("GetTransaction", $"{_txid} {_ex}", LogLevel.WARN);
                return null;
            }
        }
        #endregion

        #region Call
        private static JObject Call(string _method, params object[] _params)
        {
            try
            {
                JObject _json = new JObject();
                _json["jsonrpc"] = "2.0";
                _json["method"] = _method;
                _json["id"] = "1";

                JArray _jsonParas = new JArray();
                foreach (object _e in _params)
                {
                    if (_e is KeyValuePair<string, string>)
                    {
                        KeyValuePair<string, string> _value = (KeyValuePair<string, string>)_e;
                        JObject _jchild = new JObject();
                        _jchild[_value.Key] = _value.Value;
                        _jsonParas.Add(_jchild);
                    }
                    else if (_e is List<KeyValuePair<string, string>>)
                    {
                        List<KeyValuePair<string, string>> _childs = (List<KeyValuePair<string, string>>)_e;
                        JObject _jchild = new JObject();
                        foreach (KeyValuePair<string, string> _child in _childs) { _jchild[_child.Key] = _child.Value; }
                        _jsonParas.Add(_jchild);
                    }
                    else
                    {
                        _jsonParas.Add(_e);
                    }
                }
                _json["params"] = _jsonParas;

                if (_method == "eth_estimateGas"
                    || _method == "eth_getBalance"
                    || _method == "eth_gasPrice"
                    || _method == "eth_blockNumber"
                    || _method == "eth_sendTransaction") { Console.WriteLine(_json.ToString(Formatting.None)); }

                HttpClient _http = new HttpClient(60000);
                _http.BeginResponse("POST", Host, "");
                _http.Request.ContentType = "application/json";
                _http.EndResponse(Encoding.UTF8.GetBytes(_json.ToString(Formatting.None)));
                string _result = _http.GetResponseString(Encoding.UTF8);

                if (_method == "eth_estimateGas"
                    || _method == "eth_getBalance"
                    || _method == "eth_gasPrice"
                    || _method == "eth_blockNumber"
                    || _method == "eth_sendTransaction") { Console.WriteLine(_result); }
                _http.Dispose();

                return JObject.Parse(_result);
            }
            catch
            {
                return null;
            }
        }
        #endregion
    }
}
