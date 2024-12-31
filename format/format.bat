@echo off

SET BASEPATH=%~dp0

%JULIA_1112% --project=%BASEPATH% %BASEPATH%\format.jl