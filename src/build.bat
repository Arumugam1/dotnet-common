rem BEGIN - Setup Instructions
%windir%\Microsoft.NET\framework\v4.0.30319\msbuild.exe app\app.csproj 
if %errorlevel% neq 0 exit /b %errorlevel%
rem END - Setup Instructions
