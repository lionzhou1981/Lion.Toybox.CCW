using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Numerics;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Lion;
using Lion.Data.MySqlClient;
using Lion.Encrypt;

// Centralized Coin Wallet

namespace CCW.Core
{
    public static class Common
    {
        public static string Id = "";
        public static JObject Settings =  new JObject();
        public static string SettingsFile = "";
        public static bool Running = false;
        public static string DefaultLanguageCode = "en";
        public static int PageSize = 20;

        private static string DB_Host = "";
        private static string DB_Port = "";
        private static string DB_User = "";
        private static string DB_Pass = "";
        private static string DB_Name = "";
        private static string LogPath = "";

        public const string DateFormat_Front = "yyyy-MM-dd HH:mm";

        public static void Init(string[] _args)
        {
            Log("Sys", "Common initializing");
            Text.Words = "abcdefghijklmnopqrstuvwyz1234567890";

            Running = true;
            SettingsFile = $"{AppDomain.CurrentDomain.BaseDirectory}app{(_args.Length > 0 ? $"_{_args[0]}" : "")}.json";
            if (!File.Exists(SettingsFile)) { SettingsFile = $"{AppDomain.CurrentDomain.BaseDirectory}app.json"; }

            Settings = JObject.Parse(File.ReadAllText(SettingsFile));
            Id = Settings["Id"].Value<string>();

            if (Settings.ContainsKey("DB"))
            {
                DB_Host = Settings["DB"]["Host"].Value<string>();
                DB_Port = Settings["DB"]["Port"].Value<string>();
                DB_User = Settings["DB"]["User"].Value<string>();
                DB_Pass = Settings["DB"]["Pass"].Value<string>();
                DB_Name = Settings["DB"]["Name"].Value<string>();
            }

            if (Settings.ContainsKey("Log")) { LogPath = Settings["Log"].Value<string>() + ""; }
            Log("Sys", "Common initialized");
        }

        public static DbCommon DbCommonMain { get { return new DbCommon(DB_Host, DB_Port, DB_User, DB_Pass, DB_Name, "allowPublicKeyRetrieval=true;"); } }

        #region RandomSeed
        private static uint randomSeed = 0;
        public static uint RandomSeed { get { return randomSeed++; } }
        #endregion

        #region Log
        public static void Log(string _source, string _text, LogLevel _level = LogLevel.INFO, string _logType = "")
        {
            DateTime _now = DateTime.Now;
            string _log = $"[{_now.ToString("yyyy-MM-dd HH:mm:ss.fff")}][{_level}][{_source}]{_text}";
            Console.WriteLine(_log);

            if (LogPath == "" || _level == LogLevel.DEBUG) { return; }
            string _value = "{\"time\":\"" + _now.ToString("yyyy-MM-dd HH:mm:ss.fff") + "\",\"id\":\"" + Id + "\",\"level\":\"" + _level.ToString() + "\",\"source\":\"" + $"{(string.IsNullOrWhiteSpace(_logType) ? "" : _logType + ".") }{_source}" + "\",\"log\":\"" + _text.Replace("\n", "\\n").Replace("\r", "\\r").Replace("\"", "\\\"") + "\"}";
            File.AppendAllText($"{LogPath}/{Id}-{_now.ToString("yyyyMMdd")}.log", _value + "\n");
        }

        public static void DataLog(string _source, string _token, string _text, LogLevel _level = LogLevel.INFO)
        {
            Log(_source, _token + ":" + _text, _level, "data");
        }

        public static void DataLog(string _source, string _token, JToken _params, LogLevel _level = LogLevel.INFO)
        {
            if (_params.Type == JTokenType.Array)
            {
                var _array = _params.DeepClone() as JArray;
                _array.Add(_token);
                Log(_source, _array.ToString(Formatting.None), _level, "data");
            }
            else
            {
                var _obj = _params.DeepClone() as JObject;
                _obj["token"] = _token;
                Log(_source, _obj.ToString(Formatting.None), _level, "data");
            }
        }

        public static void OperateLog(string _source, string _token, string _text, LogLevel _level = LogLevel.INFO)
        {
            Log(_source, _token + ":" + _text, _level, "operate");
        }

        public static void OperateLog(string _source, string _token, JToken _params, LogLevel _level = LogLevel.INFO)
        {

            if (_params.Type == JTokenType.Array)
            {
                var _array = _params.DeepClone() as JArray;
                _array.Add(_token);
                Log(_source, string.Join(",", _array), _level, "operate");
            }
            else
            {
                var _obj = _params.DeepClone() as JObject;
                _obj["token"] = _token;
                Log(_source, _obj.ToString(Formatting.None), _level, "operate");
            }
        }
        #endregion

        #region SaveSettings
        public static void SaveSettings()
        {
            File.WriteAllText(SettingsFile, Settings.ToString());
        }
        #endregion

        #region Return
        public static JObject ReturnOK()
        {
            return new JObject() { ["result"] = true };
        }

        public static JObject ReturnError(int _code, string _errorMsg = "")
        {
            return new JObject() { ["errcode"] = _code, ["errormsg"] = _errorMsg };
        }

        public static JObject ReturnError(int _code, JToken _paras)
        {
            return new JObject() { ["errcode"] = _code, ["params"] = _paras };
        }
        #endregion
    }
}
