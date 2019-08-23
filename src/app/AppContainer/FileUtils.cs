using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;

namespace automation.components.data.v1.AppContainer
{
    public static class FileUtils
    {
        public static string FindFirst(string fileName, IEnumerable<string> subDirectories)
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var dirsToSearch = new string[] { baseDir }.Concat(subDirectories.Select(d => Path.Combine(baseDir, d)));

            return dirsToSearch.
                Where(dir => Directory.Exists(dir)).
                SelectMany(Finder(fileName)).
                FirstOrDefault();
        }

        private static Func<string, IEnumerable<string>> Finder(string fileName)
        {
            return (dir) =>
            {
                try
                {
                    return Directory.EnumerateFiles(dir, fileName);
                }
                catch (Exception ex) when (ex is SecurityException ||
                                           ex is UnauthorizedAccessException ||
                                           ex is IOException)
                {
                    return Enumerable.Empty<string>();
                }
            };
        }
    }
}
