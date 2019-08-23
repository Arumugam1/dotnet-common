@echo off
rem BEGIN - Remember user folder
set userdir=%cd%
cd %~dp0
rem END - Remember user folder
echo Starting Build
rem BEGIN - Build Instructions
cd..
cd src
call build.bat
rem END - Build Instructions
echo Completed Build

echo Starting Test
rem BEGIN - Build Instructions
cd..
cd test
call test.bat
rem END - Build Instructions
echo Completed Test

echo Starting to Package
rem BEGIN - Package Instructions
rem END - Package Instructions
echo Completed Packaging

REM rem BEGIN - Back to user folder
REM rem BEGIN - Back to user folder
cd %userdir%
rem END - Back to user folder
@echo on
